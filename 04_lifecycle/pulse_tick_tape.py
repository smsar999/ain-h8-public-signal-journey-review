# -*- coding: utf-8 -*-
"""V86CL R160 R2 — Source-first pulse candidate tick tape recorder.

Purpose
-------
This module records only the *candidate symbols* that matter for diagnosing
late official recommendations: GANN20 pulse rows and internal rows waiting for
R1.  It is intentionally independent from Qt and model execution.  The hot path
never waits on pandas, parquet, or SQLite; it appends compact JSONL events under
``datainfo/pulse_tick_tape``.

The recorder answers three questions that 30-minute OHLC bars cannot answer:
1. When did the pulse become known to the program?
2. When did the live price first cross frozen R1/R50/R100?
3. When did the UI/radar publish the row?
"""
from __future__ import annotations

import copy
import datetime as _dt
from collections import deque
import hashlib
import json
import math
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from market_datetime_normalizer import to_market_naive as _market_to_naive

from runtime_session import record_stage_error as _record_stage_error
from strict_jsonl import append_jsonl as _append_jsonl_strict
from r1693_3_live_truth_sync import lifecycle_ui_patch as _r16933_lifecycle_ui_patch
from pre_entry_stop_contract import (
    PRE_ENTRY_STOP_INVALIDATED, apply_invalidation as _apply_pre_entry_stop_invalidation,
    should_invalidate as _should_invalidate_pre_entry_stop,
)
from live_episode_truth_contract import (
    TRUTH_LIVE_TICK, TRUTH_R1_ACTIVATION, TRUTH_TERMINAL, stamp_truth as _stamp_episode_truth,
)
from sealed_probability_authority import is_authoritative_sealed_probability
from pulse_probability_stage_contract import (
    extract_probability as _stage_extract_probability,
    extract_levels as _stage_extract_levels,
)
from pulse_candidate_truth_contract import (
    candidate_identity_fields as _candidate_identity_fields,
    merge_candidate_truth as _merge_candidate_truth,
)
from pulse_r1_target_contract import (
    apply_r1_tick_transitions as _apply_r1_tick_transitions,
)
from r1_event_truth_projection import (
    sealed_candidate_patch as _sealed_candidate_patch,
    r1_event_truth as _r1_event_truth,
)
from durable_r1_lifecycle import (
    VERSION as DURABLE_R1_VERSION,
    R1_ACTIVE_WAITING_R50, R1_LOST_WAITING_REGAIN,
    R50_HIT_TRACKING_R100, R100_HIT_COMPLETE,
    PRE_ENTRY_STOP_INVALIDATED as DURABLE_PRE_ENTRY_STOP_INVALIDATED,
    EPISODE_EXPIRED_20_BARS, EPISODE_CLOSED_NEGATIVE_CROSS,
    TERMINAL_STATES as DURABLE_TERMINAL_STATES,
    apply_transition as _apply_durable_r1_transition,
    apply_terminal_state as _apply_durable_terminal_state,
    presentation as _durable_r1_presentation,
    is_post_r1 as _is_post_r1, is_terminal as _durable_is_terminal,
)
from durable_lifecycle_outbox import (
    enqueue as _enqueue_lifecycle_transition,
    add_projection as _add_lifecycle_projection,
    projections as _lifecycle_projections,
    owner_acknowledged as _lifecycle_owner_acknowledged,
    mark_owner_acknowledged as _mark_lifecycle_owner_acknowledged,
    mark_projection_acknowledged as _mark_lifecycle_projection_acknowledged,
    record_projection_failure as _record_lifecycle_projection_failure,
    obligations_satisfied as _lifecycle_obligations_satisfied,
    pending as _pending_lifecycle_transitions,
    acknowledge as _ack_lifecycle_transition,
    record_failure as _record_lifecycle_delivery_failure,
    has_pending as _has_pending_lifecycle_transition,
    transition_kind as _lifecycle_transition_kind,
)
from pulse_candidate_state_store import (
    store_path as _candidate_store_path, save as _save_candidate_state, load as _load_candidate_state,
)
from atomic_io_utils import write_json_atomic as _write_json_atomic
from terminal_truth_authority import (
    commit_terminal_truth as _commit_terminal_truth,
    load_terminal_truths as _load_terminal_truths,
    terminal_truth_for_episode as _terminal_truth_for_episode,
    terminal_truth_db_path as _terminal_truth_db_path,
    pending_terminal_projections as _pending_terminal_projections,
    terminal_projection_for_episode as _terminal_projection_for_episode,
    acknowledge_terminal_projection as _acknowledge_terminal_projection,
    record_terminal_projection_failure as _record_terminal_projection_failure,
    prepare_terminal_transition_intent as _prepare_terminal_transition_intent,
    clear_terminal_transition_intent as _clear_terminal_transition_intent,
    TerminalTruthError as _TerminalTruthError,
)
from durable_session_close_tombstones import (
    primary_path as _durable_tombstone_primary_path,
    flat_records as _durable_tombstone_flat_records,
    write_tombstone as _durable_write_session_close_tombstone,
    is_tombstoned as _durable_market_is_tombstoned,
)

try:
    from r159_pipeline_core import canonical_pulse_id, touch_level_state, level_reached, level_lost
except Exception:
    canonical_pulse_id = None
    touch_level_state = None
    level_reached = None
    level_lost = None

try:
    from execution_profit_layer import (
        VERSION as EXECUTION_LAYER_VERSION,
        evaluate_execution_layer as _evaluate_execution_layer,
        flatten_execution_result as _flatten_execution_result,
    )
except Exception:  # pragma: no cover - diagnostic tape must fail open
    EXECUTION_LAYER_VERSION = "execution_layer_unavailable"
    _evaluate_execution_layer = None
    def _flatten_execution_result(result):
        return {}

try:
    from gann20_episode_contract import (
        VERSION as GANN20_CONTRACT_VERSION, HORIZON_BARS, DISCARD_BELOW_PCT,
        display_threshold_pct as _display_threshold_pct,
        should_arm_r1_after_seal as _should_arm_r1_after_seal,
        canonical_episode_id as _canonical_episode_id,
        PROB_SCOPE_LIVE_CURRENT_BAR, PROB_SCOPE_SEALED_CROSS_BAR,
    )
    from gann20_episode_ledger import append_event as _append_episode_event
    from m30_sniper_birth_seal_contract import should_arm_live_r1_at_birth as _should_arm_r1_at_birth
except Exception:  # pragma: no cover
    GANN20_CONTRACT_VERSION = "unavailable"
    HORIZON_BARS = 20
    DISCARD_BELOW_PCT = 20.0
    PROB_SCOPE_LIVE_CURRENT_BAR = "LIVE_CURRENT_BAR"
    PROB_SCOPE_SEALED_CROSS_BAR = "SEALED_CROSS_BAR"
    def _display_threshold_pct(market_key): return 30.0
    def _canonical_episode_id(market_key, symbol, bar): return f"GANN20-{market_key}-{symbol}-{bar}"
    def _should_arm_r1_at_birth(p50, market_key):
        try: return 20.0 <= float(p50) < float(_display_threshold_pct(market_key))
        except Exception: return False
    def _should_arm_r1_after_seal(p50, market_key):
        try: return 20.0 <= float(p50) < float(_display_threshold_pct(market_key))
        except Exception: return False
    def _append_episode_event(*args, **kwargs): return {"written": False}

try:
    from live_sniper_contract import (
        VERSION as LIVE_SNIPER_CONTRACT_VERSION, LIVE_R1_BORN,
        ORIGIN_LIVE_PRICE_TICK, ORIGIN_HISTORICAL_REBUILD,
    )
except Exception:  # pragma: no cover
    LIVE_SNIPER_CONTRACT_VERSION = "unavailable"
    LIVE_R1_BORN = "LIVE_R1_BORN"
    ORIGIN_LIVE_PRICE_TICK = "LIVE_PRICE_TICK"
    ORIGIN_HISTORICAL_REBUILD = "HISTORICAL_REBUILD"

VERSION = "A4_2_14_RESTART_TERMINAL_ISOLATION_HOTFIX12H13_V1"


def _projection_id_for_transition(episode_id: str, event_type: str, transition_id: str) -> str:
    raw = "|".join([
        str(episode_id or "").strip(),
        str(event_type or "").strip().upper(),
        str(transition_id or "").strip(),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# Terminal outcomes owned by the live seal engine that must veto/close TickTape
# candidates.  Deliberately exclude SEALED_WAITING_R1 / SEALED_P30_CONFIRMED:
# those are terminal only for the forming engine and legitimately hand off to
# the post-seal R1 lifecycle.
_SEAL_CANDIDATE_VETO_TERMINAL_STATES = frozenset({
    "INTRABAR_CROSS_FAILED_AT_SEAL",
    "SEALED_P30_LATE_CENTER_ONLY",
    "SEALED_REJECTED",
    "TARGET_CONSUMED_BEFORE_ENTRY",
    "TARGET_CONSUMED_BEFORE_OFFICIAL",
    "LIVE_LATE_NO_CHASE",
    "LIVE_FAILED",
})


def _terminal_state_from_row(row: Mapping[str, Any]) -> str:
    source = row or {}
    for key in ("terminal_state", "episode_state", "gann20_episode_state", "live_pulse_seal_state", "action_state"):
        state = str(source.get(key) or "").strip().upper()
        if state in DURABLE_TERMINAL_STATES or state in _SEAL_CANDIDATE_VETO_TERMINAL_STATES:
            return state
    return ""


def _is_terminal(candidate: Mapping[str, Any]) -> bool:
    return bool(_durable_is_terminal(candidate) or _terminal_state_from_row(candidate))


def terminal_state_for_row(row: Mapping[str, Any]) -> str:
    """Public cross-layer terminal classifier used at candidate creation boundaries."""
    return _terminal_state_from_row(row)


def _apply_cross_layer_terminal(
    candidate: Dict[str, Any], state: str, *, row: Optional[Mapping[str, Any]] = None, at: Any = None,
) -> Dict[str, Any]:
    """Apply a terminal state even when it originates outside durable_r1_lifecycle."""
    target = str(state or "").strip().upper()
    if target in DURABLE_TERMINAL_STATES:
        _apply_durable_terminal_state(
            candidate, target,
            reason=str((row or {}).get("reason") or (row or {}).get("terminal_reason") or target),
            at=at,
        )
    elif target in _SEAL_CANDIDATE_VETO_TERMINAL_STATES:
        candidate.update({
            "episode_state": target, "gann20_episode_state": target,
            "live_pulse_seal_state": target, "action_state": target,
            "terminal_state": target,
            "terminal_reason": str((row or {}).get("reason") or (row or {}).get("terminal_reason") or target),
            "terminal_at": str(at or (row or {}).get("sealed_at") or (row or {}).get("event_time") or ""),
            "live_publishable": False, "r1_watch_armed": False, "r1_watch_mode": None,
            "truth_source": TRUTH_TERMINAL, "truth_rank": 60,
        })
    else:
        raise ValueError(f"UNSUPPORTED_CROSS_LAYER_TERMINAL_STATE:{target}")
    stamped = _stamp_episode_truth(
        candidate, truth_source=TRUTH_TERMINAL, truth_rank=60,
        stamped_at=at, producer_contract_version=VERSION,
    )
    candidate.clear(); candidate.update(stamped)
    return candidate


_LOCK = threading.RLock()
_CANDIDATES: Dict[str, Dict[str, Any]] = {}
_CANDIDATE_IDS_BY_MARKET_SYMBOL: Dict[Tuple[str, str], set[str]] = {}
_SEEN_CANDIDATE_WRITES: set[str] = set()
# Hotfix9: research JSONL is derived telemetry, never a trading authority.
_RESEARCH_TELEMETRY_QUEUE = deque()
_RESEARCH_TELEMETRY_PENDING_SIGNATURES: set[str] = set()
_RESEARCH_TELEMETRY_STATS: Dict[str, int] = {"enqueued": 0, "written": 0, "failed": 0, "dropped": 0}
_LAST_TICK_BY_SYMBOL: Dict[Tuple[str, str], Tuple[float, float]] = {}
_TERMINAL_CALLBACK = None
_LIFECYCLE_CALLBACK = None
# H12H13: candidate-scoped terminal drain is ordered, but never runs on the
# Source Truth worker.  Lifecycle/projection debt is already durable before a
# candidate id is enqueued here, so queue pressure or a crash cannot lose truth;
# the existing time-owner/restart drain remains the recovery authority.
_TERMINAL_DRAIN_CV = threading.Condition(threading.RLock())
_TERMINAL_DRAIN_QUEUE = deque()
_TERMINAL_DRAIN_PENDING: set[str] = set()
_TERMINAL_DRAIN_THREAD = None
_TERMINAL_DRAIN_STATS: Dict[str, float] = {
    "enqueued": 0, "coalesced": 0, "queue_full": 0, "completed": 0,
    "failed": 0, "inflight": 0, "queue_wait_ms_total": 0.0,
    "queue_wait_ms_max": 0.0, "processing_ms_total": 0.0,
    "processing_ms_max": 0.0,
}
_RESTORE_ATTEMPTED = False
def _episode_id_aliases(candidate_id: str) -> tuple[str, ...]:
    value = str(candidate_id or "").strip()
    if not value:
        return ()
    aliases = [value]
    # R1.2 canonical EpisodeIdentity uses an explicit millisecond token.  Older
    # pulse-engine rows used the equivalent no-``.000`` representation.  Alias
    # lookup is allowed only at this read boundary; new writes retain one legal
    # canonical key and never merge different bar identities.
    if re.search(r"T\d{6}\.000(?:-|$)", value):
        aliases.append(re.sub(r"(T\d{6})\.000(?=-|$)", r"\1", value))
    elif re.search(r"T\d{6}(?:-|$)", value):
        aliases.append(re.sub(r"(T\d{6})(?=-|$)", r"\1.000", value))
    return tuple(dict.fromkeys(aliases))



def _canonical_episode_alias(candidate_id: str) -> str:
    aliases = _episode_id_aliases(candidate_id)
    for alias in aliases:
        if re.search(r"T\d{6}\.000(?:-|$)", alias):
            return alias
    return aliases[0] if aliases else str(candidate_id or "").strip()


# A4 cross-layer monotonic terminal veto.  LivePulseSeal writes this at the
# instant an Episode becomes terminal; TickTape checks it before candidate
# creation.  The small durable store also survives a process restart.
_TERMINAL_VETO_BY_EPISODE: Dict[str, Dict[str, Any]] = {}
_TERMINAL_VETO_RESTORE_ATTEMPTED = False
_TERMINAL_VETO_RESTORE_ROOT = ""
_TERMINAL_VETO_RESTORE_ERROR = ""

# A4.2.14 Hotfix3: terminal truth and its UI/Outcome projection are two
# different durable facts.  SQLite owns a tiny terminal-only projection outbox;
# this in-memory map is merely its acceleration cache and is loaded once per
# runtime root, never queried on every tick.
_TERMINAL_PROJECTION_PENDING: Dict[str, Dict[str, Any]] = {}
_TERMINAL_PROJECTION_RESTORE_ATTEMPTED = False
_TERMINAL_PROJECTION_RESTORE_ROOT = ""



def _terminal_projection_path(authority_path: Optional[Path] = None) -> Path:
    return Path(authority_path or _terminal_truth_db_path())


def _terminal_projection_key(episode_id: str) -> str:
    return _canonical_episode_alias(str(episode_id or ""))


def _remember_terminal_projection_locked(payload: Mapping[str, Any], *, authority_path: Optional[Path] = None) -> None:
    row = dict(payload or {})
    if not bool(row.get("terminal_projection_pending", True)):
        return
    eid = _terminal_projection_key(str(row.get("episode_id") or row.get("pulse_episode_id") or ""))
    if not eid:
        return
    row["episode_id"] = eid
    row["episode_aliases"] = list(_episode_id_aliases(eid))
    row["_terminal_projection_authority_path"] = str(_terminal_projection_path(authority_path))
    _TERMINAL_PROJECTION_PENDING[eid] = row


def _ensure_terminal_projection_debts_loaded_locked() -> int:
    global _TERMINAL_PROJECTION_RESTORE_ATTEMPTED, _TERMINAL_PROJECTION_RESTORE_ROOT
    path = _terminal_projection_path()
    root = str(path)
    if _TERMINAL_PROJECTION_RESTORE_ROOT != root:
        _TERMINAL_PROJECTION_PENDING.clear()
        _TERMINAL_PROJECTION_RESTORE_ATTEMPTED = False
        _TERMINAL_PROJECTION_RESTORE_ROOT = root
    if _TERMINAL_PROJECTION_RESTORE_ATTEMPTED:
        return 0
    _TERMINAL_PROJECTION_RESTORE_ATTEMPTED = True
    loaded = 0
    try:
        for row in _pending_terminal_projections(path=path, limit=10000):
            _remember_terminal_projection_locked(row, authority_path=path)
            loaded += 1
    except Exception as exc:
        # A transient SQLite/open/read error must not permanently convert an
        # unknown projection debt into an empty queue for this process.  Keep
        # existing in-memory debt and allow the next drain/snapshot to retry.
        _TERMINAL_PROJECTION_RESTORE_ATTEMPTED = False
        _record_stage_error(
            "event_write", "restore_terminal_projection_outbox", exc,
            reason_code="TERMINAL_PROJECTION_OUTBOX_RESTORE_FAILED_RETRYABLE",
        )
    return loaded


def _terminal_projection_for_episode_locked(episode_id: str) -> Dict[str, Any]:
    for alias in _episode_id_aliases(str(episode_id or "")):
        row = _TERMINAL_PROJECTION_PENDING.get(_terminal_projection_key(alias))
        if row:
            return dict(row)
    return {}


def _terminal_projection_transition_record(payload: Mapping[str, Any]) -> Dict[str, Any]:
    row = dict(payload or {})
    eid = _terminal_projection_key(str(row.get("episode_id") or row.get("pulse_episode_id") or ""))
    state = str(row.get("terminal_state") or row.get("episode_state") or row.get("live_pulse_seal_state") or "").upper()
    at = str(row.get("terminal_at") or row.get("sealed_at") or row.get("last_update") or _now_utc().isoformat())
    reason = str(row.get("reason") or row.get("terminal_reason") or state)
    transition_id = str(row.get("transition_id") or "")
    if not transition_id:
        seed = f"{eid}\0{state}\0{at}"
        transition_id = "TERMINAL-PROJECTION-" + hashlib.sha256(seed.encode("utf-8")).hexdigest()
    candidate = dict(row)
    candidate.update({
        "candidate_id": candidate.get("candidate_id") or eid,
        "episode_id": eid,
        "pulse_episode_id": candidate.get("pulse_episode_id") or eid,
        "terminal_state": state,
        "episode_state": state,
        "live_pulse_seal_state": state,
        "gann20_episode_state": state,
        "action_state": state,
        "terminal_at": at,
        "terminal_truth_durable": True,
        "terminal_projection_authority_only": True,
        "live_publishable": False,
        "ui_patch_required": True,
        "tradable": False,
        "execution_authorized": False,
    })
    return {
        "transition_id": transition_id,
        "transition_seq": int(row.get("transition_seq") or 0),
        "previous_state": str(row.get("previous_state") or ""),
        "state": state,
        "at": at,
        "reason": reason,
        "truth_source": TRUTH_TERMINAL,
        "truth_rank": 60,
        "kind": "TERMINAL",
        "candidate": candidate,
    }


def _ack_terminal_projection_locked(payload: Mapping[str, Any]) -> bool:
    row = dict(payload or {})
    eid = _terminal_projection_key(str(row.get("episode_id") or row.get("pulse_episode_id") or ""))
    if not eid:
        return False
    path = Path(str(row.get("_terminal_projection_authority_path") or _terminal_projection_path()))
    receipt = _acknowledge_terminal_projection(eid, path=path)
    if bool(receipt.get("acknowledged")):
        _TERMINAL_PROJECTION_PENDING.pop(eid, None)
        return True
    current = _terminal_projection_for_episode(eid, path=path)
    if str(current.get("projection_status") or "").upper() == "ACKED":
        _TERMINAL_PROJECTION_PENDING.pop(eid, None)
        return True
    return False


def _record_terminal_projection_failure_locked(payload: Mapping[str, Any], error: str) -> None:
    row = dict(payload or {})
    eid = _terminal_projection_key(str(row.get("episode_id") or row.get("pulse_episode_id") or ""))
    if not eid:
        return
    path = Path(str(row.get("_terminal_projection_authority_path") or _terminal_projection_path()))
    try:
        _record_terminal_projection_failure(eid, str(error or "TERMINAL_PROJECTION_OWNER_DID_NOT_ACKNOWLEDGE"), path=path)
    except Exception as exc:
        _record_stage_error(
            "event_write", "record_terminal_projection_failure", exc,
            episode_id=eid, reason_code="TERMINAL_PROJECTION_FAILURE_RECEIPT_FAILED",
        )


class TerminalVetoStoreCorrupt(RuntimeError):
    """Raised when an existing terminal-veto store cannot be trusted."""



def _terminal_veto_store_path() -> Path:
    return _candidate_state_root() / "terminal_episode_veto.json"


def _restore_terminal_veto_locked() -> int:
    global _TERMINAL_VETO_RESTORE_ATTEMPTED, _TERMINAL_VETO_RESTORE_ROOT, _TERMINAL_VETO_RESTORE_ERROR
    path = _terminal_veto_store_path()
    try:
        store_root = str(path.resolve(strict=False))
    except Exception:
        store_root = str(path.absolute())
    if _TERMINAL_VETO_RESTORE_ROOT != store_root:
        _TERMINAL_VETO_BY_EPISODE.clear()
        _TERMINAL_VETO_RESTORE_ATTEMPTED = False
        _TERMINAL_VETO_RESTORE_ROOT = store_root
        _TERMINAL_VETO_RESTORE_ERROR = ""
    if _TERMINAL_VETO_RESTORE_ATTEMPTED:
        if _TERMINAL_VETO_RESTORE_ERROR:
            raise TerminalVetoStoreCorrupt(_TERMINAL_VETO_RESTORE_ERROR)
        return len(_TERMINAL_VETO_BY_EPISODE)
    _TERMINAL_VETO_RESTORE_ATTEMPTED = True
    try:
        # SQLite/WAL is the sole terminal authority.  The JSON file is a mirror
        # and a migration source for older releases, never a reason to erase a
        # committed terminal truth.
        legacy_rows: List[Dict[str, Any]] = []
        if path.exists():
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
                if not isinstance(payload, dict) or not isinstance(payload.get("vetoes", []), list):
                    raise ValueError("INVALID_TERMINAL_VETO_PAYLOAD")
                legacy_rows = [dict(row or {}) for row in list(payload.get("vetoes") or [])]
            except Exception as mirror_exc:
                # A corrupt mirror is tolerable only when authoritative SQLite
                # terminal rows already exist.  Otherwise fail closed.
                existing = list(_load_terminal_truths() or [])
                if not existing:
                    raise mirror_exc
                _record_stage_error(
                    "event_write", "pulse_terminal_veto_json_mirror_restore", mirror_exc,
                    reason_code="PULSE_TERMINAL_VETO_JSON_MIRROR_IGNORED",
                )
        for row in legacy_rows:
            eid = str(row.get("episode_id") or "").strip()
            state = str(row.get("terminal_state") or "").strip().upper()
            if not eid or not state:
                continue
            _commit_terminal_truth(
                eid, state, aliases=tuple(row.get("episode_aliases") or ()) or _episode_id_aliases(eid),
                market_key=str(row.get("market_key") or ""),
                symbol=str(row.get("symbol") or ""),
                at=str(row.get("terminal_at") or ""),
                reason=str(row.get("reason") or state), row=row,
            )
        _TERMINAL_VETO_BY_EPISODE.clear()
        for raw in list(_load_terminal_truths() or []):
            row = dict(raw or {})
            eid = str(row.get("episode_id") or "").strip()
            state = str(row.get("terminal_state") or "").strip().upper()
            if not eid or not state:
                continue
            aliases = tuple(row.get("episode_aliases") or ()) or _episode_id_aliases(eid)
            for alias in aliases:
                alias_text = str(alias or "").strip()
                if alias_text:
                    _TERMINAL_VETO_BY_EPISODE[alias_text] = row
    except Exception as exc:
        _TERMINAL_VETO_BY_EPISODE.clear()
        _TERMINAL_VETO_RESTORE_ERROR = f"{type(exc).__name__}:{exc}"
        _record_stage_error(
            "event_write", "pulse_terminal_veto_restore", exc,
            reason_code="PULSE_TERMINAL_VETO_RESTORE_FAILED_CLOSED",
        )
        raise TerminalVetoStoreCorrupt(_TERMINAL_VETO_RESTORE_ERROR) from exc
    return len(_TERMINAL_VETO_BY_EPISODE)


def _persist_terminal_veto_locked() -> int:
    unique: Dict[str, Dict[str, Any]] = {}
    for row in _TERMINAL_VETO_BY_EPISODE.values():
        payload = dict(row or {})
        eid = _canonical_episode_alias(str(payload.get("episode_id") or ""))
        if eid:
            payload["episode_id"] = eid
            payload["episode_aliases"] = list(_episode_id_aliases(eid))
            unique[eid] = payload
    rows = list(unique.values())
    rows.sort(key=lambda row: (str(row.get("terminal_at") or ""), str(row.get("episode_id") or "")))
    path = _terminal_veto_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(
        path, {"version": VERSION, "vetoes": rows},
        ensure_ascii=False, sort_keys=True, indent=2, trailing_newline=True,
        fsync_file=True, fsync_directory=True,
    )
    return len(rows)


def _terminal_veto_for_episode_locked(episode_id: str) -> Dict[str, Any]:
    """Return the highest-rank terminal truth for an Episode alias.

    Caller must hold ``_LOCK`` and must have restored the veto store.
    """
    eid = str(episode_id or "").strip()
    if not eid:
        return {}
    for alias in _episode_id_aliases(eid):
        row = _TERMINAL_VETO_BY_EPISODE.get(alias)
        if row:
            return dict(row)
    return {}


def terminal_veto_for_episode(episode_id: str) -> Dict[str, Any]:
    eid = str(episode_id or "").strip()
    if not eid:
        return {}
    with _LOCK:
        _restore_terminal_veto_locked()
        return _terminal_veto_for_episode_locked(eid)


def note_terminal_episode(
    episode_id: str, terminal_state: str, *, market_key: str = "", symbol: str = "",
    at: str = "", reason: str = "", row: Optional[Mapping[str, Any]] = None,
    authority_path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Commit one terminal Episode before any mutable mirror is changed.

    SQLite/WAL owns the truth.  Candidate JSON and terminal-veto JSON are
    recoverable mirrors; failure to update them cannot resurrect an Episode.
    """
    eid = str(episode_id or "").strip()
    state = str(terminal_state or "").strip().upper()
    if not eid or not state:
        return {"recorded": False, "durable": False, "reason": "EPISODE_OR_STATE_MISSING"}
    with _LOCK:
        _restore_terminal_veto_locked()
        canonical_eid = _canonical_episode_alias(eid)
        aliases = _episode_id_aliases(canonical_eid)
        terminal_at = str(at or _now_utc().isoformat())
        authority_row = {
            **dict(row or {}),
            "episode_id": canonical_eid,
            "episode_aliases": list(aliases),
            "terminal_state": state,
            "market_key": str(market_key or ""),
            "symbol": str(symbol or "").strip().upper(),
            "terminal_at": terminal_at,
            "reason": str(reason or state),
        }
        # Write-ahead terminal intent is a retry debt, not Terminal authority.
        # It is fsynced only for terminal transitions.  If SQLite fails or the
        # process dies before COMMIT, the next engine generation reconciles this
        # exact proposal before allowing the Episode to be born again.
        try:
            intent_receipt = dict(_prepare_terminal_transition_intent(
                canonical_eid, state, aliases=aliases,
                market_key=authority_row["market_key"], symbol=authority_row["symbol"],
                at=terminal_at, reason=authority_row["reason"], row=authority_row,
                path=authority_path,
            ) or {})
        except Exception as exc:
            _record_stage_error(
                "event_write", "pulse_terminal_intent_prepare", exc,
                market=authority_row["market_key"], symbol=authority_row["symbol"],
                episode_id=eid, reason_code="PULSE_TERMINAL_INTENT_PREPARE_FAILED_CLOSED",
            )
            raise
        try:
            durable_receipt = dict(_commit_terminal_truth(
                canonical_eid, state, aliases=aliases,
                market_key=authority_row["market_key"], symbol=authority_row["symbol"],
                at=terminal_at, reason=authority_row["reason"], row=authority_row,
                path=authority_path,
            ) or {})
        except Exception as exc:
            # Deliberately retain the intent.  The live object has not been
            # terminalized yet, and the transition remains retryable in-process
            # and across Restart.
            _record_stage_error(
                "event_write", "pulse_terminal_truth_commit", exc,
                market=authority_row["market_key"], symbol=authority_row["symbol"],
                episode_id=eid, reason_code="PULSE_TERMINAL_TRUTH_COMMIT_FAILED_CLOSED",
            )
            raise
        intent_cleared = bool(_clear_terminal_transition_intent(
            canonical_eid, state, path=authority_path,
        ))
        payload = {**authority_row, **durable_receipt}
        _remember_terminal_projection_locked(payload, authority_path=authority_path)
        for alias in aliases:
            _TERMINAL_VETO_BY_EPISODE[alias] = payload
        terminalized = False
        for candidate_key in aliases:
            candidate = _CANDIDATES.get(candidate_key)
            if candidate is None or _is_terminal(candidate):
                continue
            terminalized_row = _apply_cross_layer_terminal(
                candidate, state, row=dict(row or payload), at=terminal_at,
            )
            _CANDIDATES[candidate_key] = terminalized_row
            market = str(terminalized_row.get("market_key") or payload.get("market_key") or "")
            sym = str(terminalized_row.get("symbol") or payload.get("symbol") or "").strip().upper()
            if market and sym:
                _CANDIDATE_IDS_BY_MARKET_SYMBOL.setdefault((market, sym), set()).add(candidate_key)
            if market:
                _enqueue_research_jsonl(
                    _base_dir() / f"{_market_slug(market)}_pulse_candidates.jsonl",
                    terminalized_row, kind="candidate_terminal_audit",
                )
            terminalized = True
        mirror_error = ""
        try:
            _persist_terminal_veto_locked()
            _persist_candidates_locked()
        except Exception as exc:
            mirror_error = f"{type(exc).__name__}:{exc}"
            _record_stage_error(
                "event_write", "pulse_terminal_mirror_persist", exc,
                market=authority_row["market_key"], symbol=authority_row["symbol"],
                episode_id=eid, reason_code="PULSE_TERMINAL_MIRROR_PERSIST_FAILED_AUTHORITY_SAFE",
            )
        return {
            "recorded": True, "durable": True,
            "authority_durable": True, "terminal_truth_durable": True,
            "terminal_truth_payload_hash": durable_receipt.get("payload_hash"),
            "projection_persist_error": mirror_error,
            "episode_id": canonical_eid, "episode_aliases": list(aliases), "terminal_state": state,
            "candidate_terminalized": terminalized,
            "created": bool(durable_receipt.get("created")),
            "idempotent": bool(durable_receipt.get("idempotent")),
            "persist_error": mirror_error,
            "terminal_truth_version": durable_receipt.get("terminal_truth_version"),
            "terminal_intent_prepared": bool(intent_receipt.get("prepared")),
            "terminal_intent_path": intent_receipt.get("path"),
            "terminal_intent_cleared": intent_cleared,
        }


def set_lifecycle_callback(callback) -> None:
    """Register the authoritative lifecycle sink and immediately drain pending work."""
    global _LIFECYCLE_CALLBACK
    with _LOCK:
        _LIFECYCLE_CALLBACK = callback if callable(callback) else None
    if callable(callback):
        drain_lifecycle_outbox(force=True)


def set_terminal_callback(callback) -> None:
    global _TERMINAL_CALLBACK
    with _LOCK:
        _TERMINAL_CALLBACK = callback if callable(callback) else None
    if callable(callback): drain_lifecycle_outbox(force=True)


def _candidate_state_root() -> Path:
    explicit = str(os.environ.get("AIN_PULSE_TICK_TAPE_DIR", "") or "").strip()
    return Path(explicit) if explicit else Path(__file__).resolve().parent / "datainfo" / "pulse_tick_tape"


def _persist_candidates_locked() -> int:
    return _save_candidate_state(_candidate_store_path(_candidate_state_root()), _CANDIDATES.values())


def _persist_candidates_safely(operation: str, market: str = "") -> Dict[str, Any]:
    """Persist candidate truth and return an explicit durable receipt."""
    try:
        count = int(_persist_candidates_locked())
        return {
            "ok": True, "durable": True,
            "persisted_candidates": count,
            "path": str(_candidate_store_path(_candidate_state_root())),
        }
    except Exception as exc:
        _record_stage_error("event_write", operation, exc, market=str(market or ""), reason_code="PULSE_CANDIDATE_STATE_PERSIST_FAILED")
        return {
            "ok": False, "durable": False,
            "persisted_candidates": 0,
            "path": str(_candidate_store_path(_candidate_state_root())),
            "error": f"{type(exc).__name__}:{exc}",
        }


def _remove_candidate_locked(candidate_id: str) -> None:
    cid = str(candidate_id or "")
    candidate = _CANDIDATES.pop(cid, None)
    if not candidate:
        return
    key = (
        str(candidate.get("market_key") or ""),
        str(candidate.get("symbol") or "").strip().upper(),
    )
    ids = _CANDIDATE_IDS_BY_MARKET_SYMBOL.get(key)
    if ids is not None:
        ids.discard(cid)
        if not ids:
            _CANDIDATE_IDS_BY_MARKET_SYMBOL.pop(key, None)


def _candidate_truth_source(candidate: Dict[str, Any]) -> str:
    if _is_terminal(candidate) or candidate.get("pre_entry_stop_invalidated"):
        return TRUTH_TERMINAL
    return TRUTH_R1_ACTIVATION if _is_post_r1(candidate) else TRUTH_LIVE_TICK


def _transition_lifecycle(
    candidate: Dict[str, Any], *, previous_state: str, now: _dt.datetime,
    crossed_r1: bool, lost_r1: bool, regained_r1: bool,
    hit_r50: bool, hit_r100: bool, hit_stop: bool = False,
) -> Tuple[str, Dict[str, Any], Optional[Dict[str, Any]]]:
    state = _apply_durable_r1_transition(
        candidate,
        crossed_r1=crossed_r1,
        lost_r1=lost_r1,
        regained_r1=regained_r1,
        hit_r50=hit_r50,
        hit_r100=hit_r100,
        hit_stop=hit_stop,
    )
    view = _durable_r1_presentation(candidate)
    if state == previous_state:
        return state, view, None
    reason = (
        "R1_REACHED" if crossed_r1
        else "R1_LOST" if lost_r1
        else "R1_REGAINED" if regained_r1
        else "R50_REACHED" if hit_r50
        else "R100_REACHED" if hit_r100
        else state
    )
    transition = _enqueue_lifecycle_transition(
        candidate,
        previous_state=previous_state,
        state=state,
        at=now,
        reason=reason,
    )
    return state, view, transition


def _snapshot_locked() -> Dict[str, Any]:
    restored = _ensure_candidates_restored_locked()
    active = sum(1 for candidate in _CANDIDATES.values() if not _is_terminal(candidate))
    _ensure_terminal_projection_debts_loaded_locked()
    pending_outbox = sum(
        len(list(candidate.get("_lifecycle_transition_outbox") or []))
        for candidate in _CANDIDATES.values()
    )
    pending_terminal_projection = len(_TERMINAL_PROJECTION_PENDING)
    return {
        "version": VERSION,
        "lifecycle_version": DURABLE_R1_VERSION,
        "restored_candidates": restored,
        "enabled": _enabled(),
        "active_candidates": active,
        "pending_lifecycle_transitions": pending_outbox + pending_terminal_projection,
        "pending_candidate_lifecycle_transitions": pending_outbox,
        "pending_terminal_projections": pending_terminal_projection,
        "base_dir": str(_base_dir()),
        "symbols": sorted({
            str(c.get("symbol") or "")
            for c in _CANDIDATES.values()
            if c.get("symbol") and not _is_terminal(c)
        })[:1000],
    }


def _ensure_candidates_restored_locked() -> int:
    global _RESTORE_ATTEMPTED
    state_path = _candidate_store_path(_candidate_state_root())
    if _RESTORE_ATTEMPTED and (_CANDIDATES or not state_path.exists()):
        return 0
    _RESTORE_ATTEMPTED = True
    restored = 0
    reconciled_terminal = False
    try:
        # Recovery rank is explicit: a durable Terminal Veto is higher truth than
        # a possibly older candidate snapshot.  Restore vetoes *first*, then
        # reconcile every candidate before it can re-enter an active index.
        _restore_terminal_veto_locked()
        for raw_state in _load_candidate_state(state_path):
            state = dict(raw_state or {})
            cid = str(state.get("candidate_id") or state.get("episode_id") or "").strip()
            market = str(state.get("market_key") or "")
            symbol = str(state.get("symbol") or "").strip().upper()
            if not cid or not market or not symbol:
                continue
            if _market_is_session_tombstoned(market, state):
                continue
            state["candidate_id"] = cid
            state.setdefault("episode_id", cid)
            state.setdefault("pulse_episode_id", cid)
            _normalize_candidate_observed_bars(state, market_key=market)
            state["restored_from_candidate_store"] = True
            veto = _terminal_veto_for_episode_locked(cid)
            if veto and not _is_terminal(state):
                state = _apply_cross_layer_terminal(
                    state, str(veto.get("terminal_state") or "LIVE_FAILED"),
                    row=veto, at=str(veto.get("terminal_at") or ""),
                )
                state["recovered_terminal_veto_dominated_candidate_state"] = True
                reconciled_terminal = True
            _CANDIDATES[cid] = dict(state)
            if not _is_terminal(state):
                _CANDIDATE_IDS_BY_MARKET_SYMBOL.setdefault((market, symbol), set()).add(cid)
            restored += 1
        if reconciled_terminal:
            try:
                _persist_candidates_locked()
            except Exception as persist_exc:
                # Fail closed in memory.  The next restart will reconcile from the
                # durable veto again even if repairing the lower-rank snapshot fails.
                _record_stage_error(
                    "event_write", "restore_terminal_veto_reconciliation_persist", persist_exc,
                    reason_code="PULSE_TERMINAL_RECOVERY_REPAIR_PERSIST_FAILED",
                )
    except Exception as exc:
        _record_stage_error(
            "event_write", "restore_pulse_candidates", exc,
            reason_code="PULSE_CANDIDATE_STATE_RESTORE_FAILED",
        )
    return restored


def _notify_transition_callback(record: Dict[str, Any], callback: Any, *, terminal: bool) -> bool:
    if callback is None: return False
    candidate = dict(record.get("candidate") or {})
    kwargs = {
        "state": str(record.get("state") or ""), "at": record.get("at"),
        "reason": str(record.get("reason") or ""), "transition_id": str(record.get("transition_id") or ""),
        "transition_seq": int(record.get("transition_seq") or 0),
        "previous_state": str(record.get("previous_state") or ""),
    }
    if not terminal:
        kwargs.update({"truth_source": str(record.get("truth_source") or TRUTH_R1_ACTIVATION), "truth_rank": int(record.get("truth_rank") or 55)})
    try:
        result = callback(candidate, **kwargs)
        return result is not False and result is not None and result != {}
    except Exception as exc:
        _record_stage_error(
            "event_write", "pulse_tick_terminal_callback" if terminal else "pulse_tick_lifecycle_callback", exc,
            market=str(candidate.get("market_key") or ""), symbol=str(candidate.get("symbol") or ""),
            episode_id=str(candidate.get("episode_id") or candidate.get("candidate_id") or ""),
            source_observation_id=str(candidate.get("source_observation_id") or ""),
            reason_code="PULSE_TERMINAL_CALLBACK_FAILED" if terminal else "PULSE_LIFECYCLE_CALLBACK_FAILED",
        )
        return False


def _notify_lifecycle(record: Dict[str, Any]) -> bool:
    return _notify_transition_callback(record, _LIFECYCLE_CALLBACK, terminal=False)


def _notify_terminal(record: Dict[str, Any]) -> bool:
    return _notify_transition_callback(record, _TERMINAL_CALLBACK, terminal=True)


def _notify_terminal_state_owner(record: Dict[str, Any]) -> Dict[str, Any]:
    callback = getattr(_TERMINAL_CALLBACK, "_ain_terminal_state_callback", None)
    if not callable(callback):
        return {"attempted": False, "applied": False, "reason": "NO_FAST_TERMINAL_STATE_OWNER"}
    candidate = dict(record.get("candidate") or {})
    kwargs = {
        "state": str(record.get("state") or ""), "at": record.get("at"),
        "reason": str(record.get("reason") or ""),
        "transition_id": str(record.get("transition_id") or ""),
        "transition_seq": int(record.get("transition_seq") or 0),
        "previous_state": str(record.get("previous_state") or ""),
    }
    try:
        result = callback(candidate, **kwargs)
        return {
            "attempted": True,
            "applied": bool(result is not False and result is not None and result != {}),
            "transition_id": kwargs["transition_id"],
            "state": kwargs["state"],
        }
    except Exception as exc:
        _record_stage_error(
            "event_write", "pulse_tick_terminal_state_owner", exc,
            market=str(candidate.get("market_key") or ""),
            symbol=str(candidate.get("symbol") or ""),
            episode_id=str(candidate.get("episode_id") or candidate.get("candidate_id") or ""),
            source_observation_id=str(candidate.get("source_observation_id") or ""),
            reason_code="PULSE_TERMINAL_STATE_OWNER_FAILED_CLOSED",
        )
        return {
            "attempted": True, "applied": False,
            "transition_id": kwargs["transition_id"], "state": kwargs["state"],
            "error": f"{type(exc).__name__}:{exc}",
        }


def _drain_lifecycle_outbox_serial(*, force: bool = False, max_items: int = 128, candidate_id: str = "") -> Dict[str, Any]:
    """Deliver lifecycle transitions and terminal projection debt exactly once.

    Candidate transitions retain their existing durable JSON outbox.  Terminal
    projection debt is owned by the Terminal SQLite transaction and is loaded
    once per runtime root, so no SQLite read is added to the ordinary tick path.
    A terminal candidate is ACKed/removed only after: durable Terminal truth,
    owner publication ACK, and durable projection ACK.
    """
    with _LOCK:
        _ensure_candidates_restored_locked()
        _ensure_terminal_projection_debts_loaded_locked()
        due: List[Tuple[str, Dict[str, Any]]] = []
        now = _now_utc()
        candidate_filter = str(candidate_id or "").strip()
        for cid, candidate in _CANDIDATES.items():
            if candidate_filter and str(cid) != candidate_filter:
                continue
            rows = (
                sorted(
                    [dict(item or {}) for item in list(candidate.get("_lifecycle_transition_outbox") or []) if isinstance(item, dict)],
                    key=lambda item: (int(item.get("transition_seq") or 0), str(item.get("transition_id") or "")),
                )
                if force else _pending_lifecycle_transitions(candidate, now=now, limit=1)
            )
            if rows:
                due.append((cid, dict(rows[0] or {})))
            if len(due) >= max_items:
                break
    delivered = failed = removed = projection_delivered = projection_failed = 0
    r1_projection_delivered = r1_projection_failed = 0
    projection_results: List[Dict[str, Any]] = []
    changed = False
    handled_projection_ids: set[str] = set()
    for cid, record in due:
        kind = _lifecycle_transition_kind(record)
        terminal = kind == "TERMINAL"
        acknowledged = False
        owner_ok = False
        projection_ok = not terminal
        authority_ok = not terminal
        projection_payload: Dict[str, Any] = {}
        if terminal:
            with _LOCK:
                candidate_snapshot = dict(_CANDIDATES.get(cid) or record.get("candidate") or {})
                eid = str(candidate_snapshot.get("episode_id") or candidate_snapshot.get("candidate_id") or cid)
                projection_payload = _terminal_projection_for_episode_locked(eid)
            if projection_payload and str(projection_payload.get("projection_status") or "").upper() == "ACKED":
                acknowledged = authority_ok = projection_ok = True
            else:
                acknowledged = _notify_terminal(record)
                with _LOCK:
                    # A successful callback may have committed authority and
                    # inserted projection debt during this same attempt.  Read
                    # its bound authority path before checking SQLite so
                    # standalone/custom-root courts do not require a second drain.
                    projection_payload = _terminal_projection_for_episode_locked(eid) or projection_payload
                authority_path = Path(str(
                    (projection_payload or {}).get("_terminal_projection_authority_path")
                    or _terminal_projection_path()
                ))
                authority = dict(_terminal_truth_for_episode(eid, path=authority_path) or {})
                authority_ok = bool(authority.get("terminal_truth_durable"))
                with _LOCK:
                    if not projection_payload and authority_ok:
                        disk_projection = dict(_terminal_projection_for_episode(eid, path=authority_path) or {})
                        if disk_projection.get("terminal_projection_pending"):
                            _remember_terminal_projection_locked(disk_projection, authority_path=authority_path)
                            projection_payload = _terminal_projection_for_episode_locked(eid)
                    if acknowledged and authority_ok and projection_payload:
                        projection_ok = _ack_terminal_projection_locked(projection_payload)
                        if projection_ok:
                            projection_delivered += 1
                    elif acknowledged and authority_ok:
                        projection_ok = False
                handled_projection_ids.add(_terminal_projection_key(eid))
            acknowledged = bool(acknowledged and authority_ok and projection_ok)
        else:
            transition_id = str(record.get("transition_id") or "")
            owner_ok = bool(_lifecycle_owner_acknowledged(record))
            if not owner_ok:
                owner_ok = _notify_lifecycle(record)
                if owner_ok:
                    with _LOCK:
                        candidate = _CANDIDATES.get(cid)
                        if candidate is not None and _mark_lifecycle_owner_acknowledged(
                            candidate, transition_id, at=_now_utc(),
                        ):
                            changed = True

            projection_ok = True
            for projection in _lifecycle_projections(record):
                if str(projection.get("status") or "PENDING").upper() == "ACKED":
                    continue
                projection_id = str(projection.get("projection_id") or "")
                projection_type = str(projection.get("projection_type") or "").upper()
                event_type = str(projection.get("event_type") or "").upper()
                if projection_type != "GANN20_EPISODE_LEDGER":
                    projection_ok = False
                    with _LOCK:
                        candidate = _CANDIDATES.get(cid)
                        if candidate is not None and _record_lifecycle_projection_failure(
                            candidate, transition_id, projection_id,
                            error=f"UNSUPPORTED_PROJECTION_TYPE:{projection_type}", at=_now_utc(),
                        ):
                            changed = True
                    if event_type == LIVE_R1_BORN:
                        r1_projection_failed += 1
                    projection_results.append({
                        "transition_id": transition_id,
                        "projection_id": projection_id,
                        "event_type": event_type,
                        "written": False,
                        "reason": f"unsupported_projection_type:{projection_type}",
                    })
                    continue
                payload = dict(projection.get("payload") or {})
                extra = dict(projection.get("extra") or {})
                extra["projection_id"] = projection_id
                try:
                    receipt = dict(_append_episode_event(
                        event_type,
                        payload,
                        extra=extra,
                        suppress_duplicate=bool(projection.get("suppress_duplicate", True)),
                    ) or {})
                except Exception as exc:
                    receipt = {"written": False, "reason": f"{type(exc).__name__}:{exc}"}
                    _record_stage_error(
                        "event_write", "deliver_lifecycle_projection", exc,
                        market=str(payload.get("market_key") or payload.get("market") or ""),
                        symbol=str(payload.get("symbol") or ""),
                        episode_id=str(payload.get("episode_id") or payload.get("pulse_episode_id") or cid),
                        source_observation_id=str(payload.get("source_observation_id") or ""),
                        reason_code="LIFECYCLE_PROJECTION_WRITE_FAILED",
                    )
                receipt_ok = bool(
                    receipt.get("idempotent_success")
                    or (receipt.get("written") and (event_type != LIVE_R1_BORN or receipt.get("durable") is True))
                )
                if event_type == LIVE_R1_BORN and receipt.get("written") and receipt.get("durable") is not True:
                    receipt = {**receipt, "reason": receipt.get("reason") or "R1_PROJECTION_NOT_DURABLE"}
                projection_results.append({
                    "transition_id": transition_id,
                    "projection_id": projection_id,
                    "event_type": event_type,
                    **receipt,
                })
                with _LOCK:
                    candidate = _CANDIDATES.get(cid)
                    if candidate is None:
                        projection_ok = False
                        continue
                    if receipt_ok:
                        if _mark_lifecycle_projection_acknowledged(
                            candidate, transition_id, projection_id,
                            receipt=receipt, at=_now_utc(),
                        ):
                            changed = True
                        if event_type == LIVE_R1_BORN:
                            r1_projection_delivered += 1
                    else:
                        projection_ok = False
                        if _record_lifecycle_projection_failure(
                            candidate, transition_id, projection_id,
                            error=receipt.get("reason") or "PROJECTION_NOT_ACKNOWLEDGED",
                            at=_now_utc(),
                        ):
                            changed = True
                        if event_type == LIVE_R1_BORN:
                            r1_projection_failed += 1

            with _LOCK:
                candidate = _CANDIDATES.get(cid)
                refreshed_record = None
                if candidate is not None:
                    for item in list(candidate.get("_lifecycle_transition_outbox") or []):
                        if isinstance(item, dict) and str(item.get("transition_id") or "") == transition_id:
                            refreshed_record = dict(item)
                            break
                acknowledged = bool(
                    candidate is not None
                    and refreshed_record is not None
                    and owner_ok
                    and projection_ok
                    and _lifecycle_obligations_satisfied(refreshed_record)
                )

        with _LOCK:
            candidate = _CANDIDATES.get(cid)
            if candidate is None:
                continue
            transition_id = str(record.get("transition_id") or "")
            if acknowledged:
                if _ack_lifecycle_transition(candidate, transition_id, acked_at=_now_utc()):
                    delivered += 1
                    changed = True
                if _is_terminal(candidate) and not _has_pending_lifecycle_transition(candidate):
                    _remove_candidate_locked(cid)
                    removed += 1
            else:
                if terminal and projection_payload:
                    _record_terminal_projection_failure_locked(
                        projection_payload,
                        "TERMINAL_AUTHORITY_OR_PROJECTION_NOT_ACKNOWLEDGED",
                    )
                failure_reason = (
                    "TERMINAL_AUTHORITY_OR_PROJECTION_NOT_ACKNOWLEDGED"
                    if terminal else (
                        "LIFECYCLE_PROJECTION_NOT_ACKNOWLEDGED"
                        if bool(owner_ok) and not bool(projection_ok)
                        else "LIFECYCLE_OWNER_DID_NOT_ACKNOWLEDGE"
                    )
                )
                if _record_lifecycle_delivery_failure(
                    candidate, transition_id,
                    error=failure_reason,
                    at=_now_utc(),
                ):
                    failed += 1
                    changed = True

    # Replay authority-only debt left by a crash after COMMIT but before the
    # candidate mirror/journal/UI path.  Exclude debts handled above.
    with _LOCK:
        authority_due = [
            dict(row) for eid, row in sorted(_TERMINAL_PROJECTION_PENDING.items())
            if eid not in handled_projection_ids
        ][:max(0, int(max_items) - len(due))]
    for payload in authority_due:
        record = _terminal_projection_transition_record(payload)
        acknowledged = _notify_terminal(record)
        with _LOCK:
            if acknowledged and _ack_terminal_projection_locked(payload):
                projection_delivered += 1
                delivered += 1
            else:
                projection_failed += 1
                failed += 1
                _record_terminal_projection_failure_locked(
                    payload, "TERMINAL_PROJECTION_OWNER_DID_NOT_ACKNOWLEDGE",
                )

    if changed or removed:
        with _LOCK:
            _persist_candidates_safely("persist_lifecycle_outbox_delivery")
    return {
        "attempted": len(due) + len(authority_due),
        "delivered": delivered,
        "failed": failed,
        "terminal_candidates_removed": removed,
        "terminal_projection_delivered": projection_delivered,
        "terminal_projection_failed": projection_failed,
        "r1_projection_delivered": r1_projection_delivered,
        "r1_projection_failed": r1_projection_failed,
        "projection_results": projection_results,
    }


# H12H13: all lifecycle/terminal debt delivery is single-flight.  The dedicated
# terminal accelerator and the ordinary Time Owner may wake at the same time,
# but they must never invoke the same owner/projection side effect concurrently.
# This lock is deliberately outside the Source Truth lane: record_price_batch()
# only enqueues durable debt and never waits on it.
_LIFECYCLE_DRAIN_SERIAL_LOCK = threading.RLock()

def drain_lifecycle_outbox(*, force: bool = False, max_items: int = 128, candidate_id: str = "") -> Dict[str, Any]:
    with _LIFECYCLE_DRAIN_SERIAL_LOCK:
        return _drain_lifecycle_outbox_serial(
            force=force, max_items=max_items, candidate_id=candidate_id,
        )


def _drain_terminal_candidate_outbox(candidate_id: str, *, max_transitions: int = 16) -> Dict[str, Any]:
    """Drain one already-durable terminal candidate in causal transition order.

    A post-R1 stop can have an older R1 transition ahead of STOPPED_OUT.  One
    global drain may therefore publish the stale owner state and defer terminal
    truth.  This helper is candidate-scoped and is called only for safety-terminal
    transitions after candidate persistence; normal R1/live work remains async.
    """
    cid = str(candidate_id or "").strip()
    summary = {
        "attempted": 0, "delivered": 0, "failed": 0,
        "terminal_candidates_removed": 0,
        "terminal_projection_delivered": 0,
        "terminal_projection_failed": 0,
        "r1_projection_delivered": 0,
        "r1_projection_failed": 0,
        "projection_results": [],
    }
    if not cid:
        return summary
    for _ in range(max(1, int(max_transitions or 1))):
        with _LOCK:
            candidate = _CANDIDATES.get(cid)
            if candidate is None:
                break
            pending_before = len(list(candidate.get("_lifecycle_transition_outbox") or []))
        if pending_before <= 0:
            break
        result = dict(drain_lifecycle_outbox(force=True, max_items=1, candidate_id=cid) or {})
        for key in (
            "attempted", "delivered", "failed", "terminal_candidates_removed",
            "terminal_projection_delivered", "terminal_projection_failed",
            "r1_projection_delivered", "r1_projection_failed",
        ):
            summary[key] = int(summary.get(key, 0) or 0) + int(result.get(key, 0) or 0)
        summary["projection_results"].extend(list(result.get("projection_results") or []))
        with _LOCK:
            candidate_after = _CANDIDATES.get(cid)
            pending_after = len(list(candidate_after.get("_lifecycle_transition_outbox") or [])) if candidate_after else 0
        if candidate_after is None or pending_after <= 0 or pending_after >= pending_before:
            break
    return summary





def _terminal_drain_queue_max() -> int:
    try:
        return max(32, min(8192, int(os.getenv("AIN_TERMINAL_PROJECTION_QUEUE_MAX", "1024") or 1024)))
    except Exception:
        return 1024


def _terminal_projection_worker() -> None:
    # H12H14H3 freeze court robustness: bind the synchronization objects once per
    # worker generation.  Production never reloads this module, but adversarial
    # courts intentionally do; resolving globals again between ``with`` and
    # ``wait`` can pair an old acquired lock with a newly reloaded Condition and
    # raise ``cannot wait on un-acquired lock`` in an orphan daemon thread.
    cv = _TERMINAL_DRAIN_CV
    queue = _TERMINAL_DRAIN_QUEUE
    pending = _TERMINAL_DRAIN_PENDING
    stats = _TERMINAL_DRAIN_STATS
    while True:
        with cv:
            while not queue:
                cv.wait(timeout=0.5)
            cid, enqueued_at = queue.popleft()
            stats["inflight"] = 1
        wait_ms = max(0.0, (time.monotonic() - float(enqueued_at)) * 1000.0)
        started = time.monotonic()
        try:
            _drain_terminal_candidate_outbox(cid)
            with cv:
                stats["completed"] += 1
        except Exception as exc:
            with cv:
                stats["failed"] += 1
            _record_stage_error(
                "event_write", "terminal_projection_async_worker", exc,
                episode_id=str(cid or ""), reason_code="TERMINAL_ASYNC_PROJECTION_FAILED",
            )
        finally:
            processing_ms = max(0.0, (time.monotonic() - started) * 1000.0)
            with cv:
                pending.discard(cid)
                stats["inflight"] = 0
                stats["queue_wait_ms_total"] += wait_ms
                stats["queue_wait_ms_max"] = max(float(stats["queue_wait_ms_max"]), wait_ms)
                stats["processing_ms_total"] += processing_ms
                stats["processing_ms_max"] = max(float(stats["processing_ms_max"]), processing_ms)
                cv.notify_all()


def quiesce_terminal_projection_scheduler(timeout_sec: float = 5.0) -> bool:
    """Wait until terminal projection debt has no queued/in-flight accelerator work.

    The worker remains a daemon and may be reused later; quiescence only proves
    that no terminal projection I/O is still touching the current runtime root.
    Durable debt remains the restart authority if the timeout expires.
    """
    try:
        timeout = max(0.0, float(timeout_sec))
    except Exception:
        timeout = 5.0
    deadline = time.monotonic() + timeout
    with _TERMINAL_DRAIN_CV:
        while _TERMINAL_DRAIN_QUEUE or _TERMINAL_DRAIN_PENDING or int(_TERMINAL_DRAIN_STATS.get("inflight", 0) or 0):
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                return False
            _TERMINAL_DRAIN_CV.wait(timeout=min(0.05, remaining))
        return True


def _ensure_terminal_projection_worker_locked() -> None:
    global _TERMINAL_DRAIN_THREAD
    if _TERMINAL_DRAIN_THREAD is not None and _TERMINAL_DRAIN_THREAD.is_alive():
        return
    thread = threading.Thread(
        target=_terminal_projection_worker, daemon=True,
        name="AinTerminalProjectionDrain",
    )
    _TERMINAL_DRAIN_THREAD = thread
    thread.start()


def _enqueue_terminal_candidate_drain(candidate_id: str) -> Dict[str, Any]:
    cid = str(candidate_id or "").strip()
    if not cid:
        return {"queued": False, "reason": "MISSING_CANDIDATE_ID"}
    with _TERMINAL_DRAIN_CV:
        if cid in _TERMINAL_DRAIN_PENDING:
            _TERMINAL_DRAIN_STATS["coalesced"] += 1
            return {"queued": True, "coalesced": True, "candidate_id": cid}
        if len(_TERMINAL_DRAIN_QUEUE) >= _terminal_drain_queue_max():
            _TERMINAL_DRAIN_STATS["queue_full"] += 1
            # Debt remains durable and will be retried by the normal time-owner.
            return {"queued": False, "reason": "TERMINAL_PROJECTION_QUEUE_FULL", "candidate_id": cid}
        _TERMINAL_DRAIN_PENDING.add(cid)
        _TERMINAL_DRAIN_QUEUE.append((cid, time.monotonic()))
        _TERMINAL_DRAIN_STATS["enqueued"] += 1
        _ensure_terminal_projection_worker_locked()
        _TERMINAL_DRAIN_CV.notify()
        return {"queued": True, "coalesced": False, "candidate_id": cid}


def terminal_projection_scheduler_snapshot() -> Dict[str, Any]:
    with _TERMINAL_DRAIN_CV:
        completed = int(_TERMINAL_DRAIN_STATS.get("completed", 0) or 0)
        failed = int(_TERMINAL_DRAIN_STATS.get("failed", 0) or 0)
        count = max(1, completed + failed)
        return {
            **dict(_TERMINAL_DRAIN_STATS),
            "pending": len(_TERMINAL_DRAIN_QUEUE),
            "dedup_pending": len(_TERMINAL_DRAIN_PENDING),
            "queue_wait_ms_mean": float(_TERMINAL_DRAIN_STATS.get("queue_wait_ms_total", 0.0) or 0.0) / count,
            "processing_ms_mean": float(_TERMINAL_DRAIN_STATS.get("processing_ms_total", 0.0) or 0.0) / count,
            "thread_alive": bool(_TERMINAL_DRAIN_THREAD is not None and _TERMINAL_DRAIN_THREAD.is_alive()),
        }

def active_candidate_rows(market_key: str = "") -> List[Dict[str, Any]]:
    """Return durable active candidate truth for a session-close write-ahead intent."""
    market = str(market_key or "")
    with _LOCK:
        _ensure_candidates_restored_locked()
        return [
            dict(candidate)
            for candidate in _CANDIDATES.values()
            if (not market or str(candidate.get("market_key") or "") == market)
            and not _is_terminal(candidate)
            and not _market_is_session_tombstoned(str(candidate.get("market_key") or ""), candidate)
        ]


def project_terminal_rows(
    rows: Iterable[Mapping[str, Any]], *, state: str,
    reason: str = "session_close", at: Any = None,
) -> List[Dict[str, Any]]:
    """Purely project saved active candidates to terminal rows for crash recovery."""
    projected: List[Dict[str, Any]] = []
    for raw in rows or []:
        if not isinstance(raw, Mapping):
            continue
        candidate = dict(raw)
        previous = str(candidate.get("episode_state") or candidate.get("gann20_episode_state") or "")
        _apply_durable_terminal_state(candidate, state, reason=reason, at=at)
        candidate["previous_state"] = previous
        candidate["session_close_reconstructed_from_intent"] = True
        candidate["_lifecycle_transition_outbox"] = []
        projected.append(candidate)
    return projected

def has_active_candidate_for_symbol(market_key: str, symbol: str) -> bool:
    """Cheap decision-bearing check for source-journal durability gating."""
    market = str(market_key or "")
    sym = str(symbol or "").strip().upper()
    if not market or not sym:
        return False
    with _LOCK:
        _ensure_candidates_restored_locked()
        for cid in tuple(_CANDIDATE_IDS_BY_MARKET_SYMBOL.get((market, sym), set())):
            candidate = _CANDIDATES.get(cid)
            if candidate and not _is_terminal(candidate) and not _market_is_session_tombstoned(market, candidate):
                return True
        return False


def active_markets() -> List[str]:
    """Return markets that still own active tick candidates."""
    with _LOCK:
        _ensure_candidates_restored_locked()
        return sorted({
            str(candidate.get("market_key") or "")
            for candidate in _CANDIDATES.values()
            if candidate.get("market_key") and not _is_terminal(candidate) and not _market_is_session_tombstoned(str(candidate.get("market_key") or ""), candidate)
        })


def _session_close_tombstone_path() -> Path:
    return _durable_tombstone_primary_path()


def _load_session_close_tombstones() -> Dict[str, str]:
    # Corruption is intentionally propagated.  Candidate restoration and new
    # registration must fail closed rather than resurrect a prior session.
    return dict(_durable_tombstone_flat_records())


def _write_session_close_tombstone(market: str, at: Any) -> Dict[str, Any]:
    return dict(_durable_write_session_close_tombstone(str(market or ""), at) or {})


def ensure_session_close_tombstone(market: str, at: Any) -> Dict[str, Any]:
    """Public idempotent authority used by the close transaction coordinator."""
    return _write_session_close_tombstone(market, at)


def _market_is_session_tombstoned(market: str, candidate: Mapping[str, Any] | None = None) -> bool:
    return bool(_durable_market_is_tombstoned(str(market or ""), candidate))



def finalize_session_episodes(
    market_key: str, episode_ids: Iterable[str], *, at: Any, session_date: str = "",
    state: str = "EXPIRED_SESSION_CLOSE", reason: str = "session_close",
) -> Dict[str, Any]:
    """Finalize only the immutable episodes captured by one close transaction.

    A debt from an older session must never close a newly-created candidate in
    the same market.  The caller supplies the exact episode ids captured before
    any owner mutation.
    """
    market = str(market_key or "")
    wanted = {str(value or "").strip() for value in episode_ids or [] if str(value or "").strip()}
    try:
        tombstone = _write_session_close_tombstone(market, at)
    except Exception as exc:
        _record_stage_error(
            "event_write", "persist_scoped_session_close_tombstone", exc,
            market=market, reason_code="SESSION_CLOSE_TOMBSTONE_FAILED",
        )
        return {
            "accepted": False, "market_key": market, "state": str(state or ""),
            "finalized": 0, "removed": 0, "persisted": False,
            "target_episode_ids": sorted(wanted), "error": f"{type(exc).__name__}:{exc}",
        }
    terminal_rows: List[Dict[str, Any]] = []
    ids: List[str] = []
    found: set[str] = set()
    with _LOCK:
        _ensure_candidates_restored_locked()
        for candidate_id, candidate in list(_CANDIDATES.items()):
            if str(candidate.get("market_key") or "") != market:
                continue
            episode = str(
                candidate.get("pulse_episode_id") or candidate.get("episode_id")
                or candidate.get("candidate_id") or candidate_id or ""
            ).strip()
            if episode not in wanted:
                continue
            found.add(episode)
            if _is_terminal(candidate):
                # A previous attempt may have persisted terminal truth but failed
                # before cleanup.  Re-persist and remove it idempotently now.
                terminal_rows.append(dict(candidate)); ids.append(str(candidate_id))
                continue
            previous = str(candidate.get("episode_state") or candidate.get("gann20_episode_state") or "")
            obsolete = len(list(candidate.get("_lifecycle_transition_outbox") or []))
            _apply_durable_terminal_state(candidate, state, reason=reason, at=at)
            candidate["previous_state"] = previous
            candidate["session_close_obsolete_transition_count"] = obsolete
            candidate["session_close_transaction_scoped"] = True
            candidate["session_close_session_date"] = str(session_date or "")
            candidate["_lifecycle_transition_outbox"] = []
            terminal_rows.append(dict(candidate)); ids.append(str(candidate_id))
        terminal_receipt = _persist_candidates_safely("persist_scoped_session_terminal_truth", market)
        if not terminal_receipt.get("ok"):
            return {
                "accepted": False, "market_key": market, "state": str(state or ""),
                "finalized": len(terminal_rows), "removed": 0, "rows": terminal_rows,
                "persisted": False, "tombstone": tombstone,
                "target_episode_ids": sorted(wanted), "found_episode_ids": sorted(found),
                "error": terminal_receipt.get("error"),
            }
        for cid in ids:
            _remove_candidate_locked(cid)
        cleanup_receipt = _persist_candidates_safely("persist_scoped_session_cleanup", market)
    return {
        "accepted": bool(cleanup_receipt.get("ok")), "market_key": market,
        "state": str(state or ""), "finalized": len(terminal_rows), "removed": len(ids),
        "rows": terminal_rows, "persisted": bool(cleanup_receipt.get("ok")),
        "terminal_persisted": True, "tombstone": tombstone,
        "target_episode_ids": sorted(wanted), "found_episode_ids": sorted(found),
        "missing_episode_ids": sorted(wanted - found),
        "candidate_store_path": cleanup_receipt.get("path"),
    }

def finalize_market_session(
    market_key: str, *, at: Any,
    state: str = "EXPIRED_SESSION_CLOSE", reason: str = "session_close",
) -> Dict[str, Any]:
    """Compatibility market-wide close built from the scoped authority."""
    market = str(market_key or "")
    rows = active_candidate_rows(market)
    ids = [
        str(row.get("pulse_episode_id") or row.get("episode_id") or row.get("candidate_id") or "")
        for row in rows
    ]
    return finalize_session_episodes(
        market, ids, at=at, state=state, reason=reason,
    )


def _enabled() -> bool:
    return str(os.environ.get("AIN_PULSE_TICK_TAPE_ENABLED", "1")).strip().lower() not in {"0", "false", "no", "off"}


def _max_candidates() -> int:
    try:
        return max(32, int(os.environ.get("AIN_PULSE_TICK_TAPE_MAX_CANDIDATES", "512") or 512))
    except Exception:
        return 512


def _min_tick_interval_sec() -> float:
    # 0 means record every changed tick for candidates.  A tiny default protects
    # slow disks while preserving the first R1/R50/R100 transitions.
    try:
        return max(0.0, float(os.environ.get("AIN_PULSE_TICK_TAPE_MIN_TICK_SEC", "0.20") or 0.20))
    except Exception:
        return 0.20


def _now_utc() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


def _safe_float(v: Any, default: float = math.nan) -> float:
    try:
        if v is None:
            return default
        if isinstance(v, str):
            s = v.strip().replace("%", "").replace(",", "")
            if not s or s in {"-", "nan", "None"}:
                return default
            # Accept Arabic text fields by extracting first number when needed.
            m = re.search(r"[-+]?\d+(?:\.\d+)?", s)
            if m:
                return float(m.group(0))
            return default
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _text(row: Dict[str, Any], *keys: str) -> str:
    parts: List[str] = []
    source = row if isinstance(row, dict) else {}
    for key in keys:
        value = source.get(key)
        if value is not None:
            parts.append(str(value))
    return " | ".join(parts)


def _extract_probability(
    row: Dict[str, Any], *, prefer_sealed: bool = False,
) -> Tuple[float, float]:
    return _stage_extract_probability(row, prefer_sealed=prefer_sealed)


def _extract_levels(
    row: Dict[str, Any], *, prefer_sealed: bool = False,
) -> Dict[str, float]:
    return _stage_extract_levels(row, prefer_sealed=prefer_sealed)


def _market_slug(market_key: str) -> str:
    s = str(market_key or "market").strip()
    if "السعود" in s or "sa" in s.lower():
        return "saudi_local"
    if "الأمري" in s or "us" in s.lower():
        return "us_local" if "local" in s.lower() or "local_" in s else "us_api"
    return re.sub(r"[^A-Za-z0-9_\-]+", "_", s)[:80] or "market"


def _base_dir() -> Path:
    root = Path(os.environ.get("AIN_PULSE_TICK_TAPE_DIR", "") or Path(__file__).resolve().parent / "datainfo" / "pulse_tick_tape")
    day = _dt.datetime.now().strftime("%Y-%m-%d")
    return root / day


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> bool:
    try:
        return bool(_append_jsonl_strict(Path(path), dict(payload or {})))
    except Exception as exc:
        _record_stage_error(
            "event_write", "pulse_tick_tape_append", exc,
            market=str(payload.get("market_key") or payload.get("market") or ""),
            symbol=str(payload.get("symbol") or ""),
            episode_id=str(payload.get("pulse_episode_id") or payload.get("candidate_id") or ""),
            source_observation_id=str(payload.get("source_observation_id") or ""),
            reason_code="PULSE_TICK_JSONL_APPEND_FAILED",
        )
        return False


def _research_telemetry_limit() -> int:
    try:
        return max(256, int(os.environ.get("AIN_PULSE_RESEARCH_TELEMETRY_QUEUE_MAX", "8192") or 8192))
    except Exception:
        return 8192


def _enqueue_research_jsonl(path: Path, payload: Mapping[str, Any], *, kind: str = "research", signature: str = "") -> Dict[str, Any]:
    """Queue derived research JSONL without letting I/O gate trading truth."""
    sig = str(signature or "")
    with _LOCK:
        if sig and (sig in _SEEN_CANDIDATE_WRITES or sig in _RESEARCH_TELEMETRY_PENDING_SIGNATURES):
            return {"queued": False, "duplicate": True, "kind": str(kind or "research")}
        limit = _research_telemetry_limit()
        while len(_RESEARCH_TELEMETRY_QUEUE) >= limit:
            dropped = dict(_RESEARCH_TELEMETRY_QUEUE.popleft() or {})
            dropped_sig = str(dropped.get("signature") or "")
            if dropped_sig:
                _RESEARCH_TELEMETRY_PENDING_SIGNATURES.discard(dropped_sig)
            _RESEARCH_TELEMETRY_STATS["dropped"] = int(_RESEARCH_TELEMETRY_STATS.get("dropped") or 0) + 1
        row = {"path": str(Path(path)), "payload": dict(payload or {}), "kind": str(kind or "research"), "signature": sig, "attempts": 0}
        _RESEARCH_TELEMETRY_QUEUE.append(row)
        if sig:
            _RESEARCH_TELEMETRY_PENDING_SIGNATURES.add(sig)
        _RESEARCH_TELEMETRY_STATS["enqueued"] = int(_RESEARCH_TELEMETRY_STATS.get("enqueued") or 0) + 1
        return {"queued": True, "duplicate": False, "kind": row["kind"], "pending": len(_RESEARCH_TELEMETRY_QUEUE)}


def drain_research_telemetry(*, max_items: int = 512) -> Dict[str, Any]:
    """Drain non-authoritative research telemetry behind the live path."""
    attempted = written = failed = 0
    for _ in range(max(0, int(max_items))):
        with _LOCK:
            if not _RESEARCH_TELEMETRY_QUEUE:
                break
            item = dict(_RESEARCH_TELEMETRY_QUEUE.popleft() or {})
        attempted += 1
        ok = _append_jsonl(Path(str(item.get("path") or "")), dict(item.get("payload") or {}))
        sig = str(item.get("signature") or "")
        if ok:
            written += 1
            with _LOCK:
                if sig:
                    _RESEARCH_TELEMETRY_PENDING_SIGNATURES.discard(sig)
                    _SEEN_CANDIDATE_WRITES.add(sig)
                _RESEARCH_TELEMETRY_STATS["written"] = int(_RESEARCH_TELEMETRY_STATS.get("written") or 0) + 1
            continue
        failed += 1
        item["attempts"] = int(item.get("attempts") or 0) + 1
        with _LOCK:
            _RESEARCH_TELEMETRY_STATS["failed"] = int(_RESEARCH_TELEMETRY_STATS.get("failed") or 0) + 1
            if int(item["attempts"]) < 8 and len(_RESEARCH_TELEMETRY_QUEUE) < _research_telemetry_limit():
                _RESEARCH_TELEMETRY_QUEUE.append(item)
            else:
                if sig:
                    _RESEARCH_TELEMETRY_PENDING_SIGNATURES.discard(sig)
                _RESEARCH_TELEMETRY_STATS["dropped"] = int(_RESEARCH_TELEMETRY_STATS.get("dropped") or 0) + 1
    with _LOCK:
        pending = len(_RESEARCH_TELEMETRY_QUEUE)
        stats = dict(_RESEARCH_TELEMETRY_STATS)
    return {"attempted": attempted, "written": written, "failed": failed, "pending": pending, "stats": stats, "authoritative": False}


def _candidate_id(market_key: str, row: Dict[str, Any]) -> str:
    source = dict(row or {})
    explicit_episode = str(source.get("pulse_episode_id") or source.get("episode_id") or "").strip()
    if explicit_episode:
        return explicit_episode
    symbol = str(source.get("symbol") or source.get("code") or "").strip().upper()
    bar = str(
        source.get("signal_bar_time") or source.get("pulse_bar_time")
        or source.get("signal_time") or source.get("gann_anchor_time")
        or source.get("recommendation_datetime") or ""
    ).strip()
    if symbol and bar:
        # PulseTickTape keeps the protected internal pulse identity contract.
        # Legal lifecycle/audit projections may expose the `.000` alias, but a
        # row without an explicit episode id must not silently switch the tape
        # key to that external alias.
        return str(_canonical_episode_id(market_key, symbol, bar))
    for key in ("identifier", "signal_id", "row_id", "id"):
        value = str(source.get(key) or "").strip()
        if value and value != "-":
            return value
    return str(_canonical_episode_id(market_key, symbol or "NA", bar or "UNKNOWN"))


def _is_candidate_row(row: Dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    symbol = str(row.get("symbol") or "").strip()
    if not symbol:
        return False
    p50, _p100 = _extract_probability(row)
    txt = _text(row, "source", "system_id", "system_family", "reason", "var3_gann_category_text", "radar_stage", "stock_rating", "action_state")
    looks_gann = "gann20" in txt.lower() or "R50/20" in txt or "R100/20" in txt or math.isfinite(p50)
    if not looks_gann:
        return False
    # Watch all visible probability cases >=20 and explicit R1-waiting rows.  This is
    # diagnostic only, not a recommendation gate.
    if math.isfinite(p50) and p50 >= 20.0:
        return True
    low = txt.lower()
    if "waiting_r1" in low or "ينتظر r1" in low or "r1" in low:
        return True
    return False


def _apply_candidate_probability_geometry(
    state: Dict[str, Any], row: Dict[str, Any], levels: Dict[str, Any], *,
    market: str, source: str, p50: float, p100: float, is_sealed_update: bool,
    probability_scope: str, episode_state: str, now: _dt.datetime,
) -> None:
    """Keep immutable BIRTH geometry separate from the later sealed model grid."""
    scope_live = str(probability_scope or "").upper()
    state_live = str(episode_state or "").upper()
    historical = bool(
        row.get("snapshot_review_only") or row.get("saved_snapshot_review")
        or row.get("historical_reconstructed") or row.get("r1_activation_reconstructed")
        or str(source or "").lower().startswith("replay")
    )
    live_arm = bool(
        not is_sealed_update and not historical
        and scope_live == str(PROB_SCOPE_LIVE_CURRENT_BAR).upper()
        and state_live == "LIVE_WAITING_R1" and bool(row.get("r1_watch_armed"))
        and _should_arm_r1_at_birth(p50, market)
        and math.isfinite(_safe_float(state.get("live_r1_frozen"), math.nan))
        and bool(state.get("birth_source_observation_id"))
    )
    if live_arm and not state.get("activated_r1"):
        state.update({
            "r1_watch_armed": True, "r1_watch_mode": "LIVE_BIRTH_R1",
            "r1_watch_armed_at": row.get("r1_watch_armed_at") or now.isoformat(),
            "r1_watch_source": "LIVE_WAITING_R1", "r1_frozen": state.get("live_r1_frozen"),
            "r50_frozen": state.get("live_r50_frozen"), "r100_frozen": state.get("live_r100_frozen"),
            "stop_frozen": state.get("live_stop_frozen"), "pulse_anchor_price": state.get("live_anchor_price"),
            "p50_frozen": p50, "p100_frozen": p100,
        })
        _append_episode_event("LIVE_R1_WATCH_ARMED", state, extra={"event_origin": "LIVE_SOURCE_OBSERVATION"}, suppress_duplicate=True)
    if not is_sealed_update:
        return
    _sealed_close = _safe_float(
        row.get("sealed_signal_bar_close", row.get("signal_bar_close", levels.get("signal_bar_close"))),
        math.nan,
    )
    state.update(_sealed_candidate_patch(row, levels, state, p50=p50, p100=p100))
    strict_arm = bool(
        not historical
        and str(row.get("probability_scope_sealed") or probability_scope or "").upper() == str(PROB_SCOPE_SEALED_CROSS_BAR).upper()
        and str(episode_state or "").upper() == "SEALED_WAITING_R1"
        and bool(row.get("r1_watch_armed", True)) and _should_arm_r1_after_seal(p50, market)
        and math.isfinite(_safe_float(state.get("sealed_r1_frozen"), math.nan))
    )
    state.update({
        "r1_watch_armed": strict_arm,
        "r1_watch_armed_at": now.isoformat() if strict_arm else state.get("r1_watch_armed_at"),
        "r1_watch_mode": "SEALED_R1" if strict_arm else state.get("r1_watch_mode"),
        "r1_watch_source": "SEALED_WAITING_R1" if strict_arm else state.get("r1_watch_source", "NOT_ARMED"),
        "historical_reconstructed": historical, "p50_frozen": p50, "p100_frozen": p100,
        "r1_frozen": state.get("sealed_r1_frozen"), "r50_frozen": state.get("sealed_r50_frozen"),
        "r100_frozen": state.get("sealed_r100_frozen"), "stop_frozen": state.get("sealed_stop_frozen"),
        "pulse_anchor_price": state.get("sealed_anchor_price"),
    })
    _append_episode_event("TICK_TAPE_SEALED_GEOMETRY", state, extra={"event_origin": "SEALED_CROSS_BAR"}, suppress_duplicate=True)


def register_candidates(market_key: str, rows: Iterable[Dict[str, Any]], source: str = "unknown") -> Dict[str, Any]:
    if not _enabled():
        return {"enabled": False, "registered": 0}
    registered = 0
    registered_ids: List[str] = []
    market = str(market_key or "")
    if _market_is_session_tombstoned(market):
        return {"enabled": True, "written": 0, "active": snapshot().get("active_candidates", 0), "session_closed": True}
    slug = _market_slug(market)
    now = _now_utc()
    cand_path = _base_dir() / f"{slug}_pulse_candidates.jsonl"
    candidate_audit_intents: List[Tuple[str, Dict[str, Any]]] = []
    with _LOCK:
        _ensure_candidates_restored_locked()
        _restore_terminal_veto_locked()
        for raw in rows or []:
            try:
                row = dict(raw or {})
                symbol = str(row.get("symbol") or "").strip().upper()
                cid = _candidate_id(market, row)
                terminal_state = _terminal_state_from_row(row)
                if terminal_state and symbol:
                    receipt = note_terminal_episode(
                        cid, terminal_state, market_key=market, symbol=symbol,
                        at=now.isoformat(), reason=str(row.get("reason") or row.get("terminal_reason") or terminal_state),
                        row=row,
                    )
                    if receipt.get("candidate_terminalized"):
                        registered += 1
                        registered_ids.append(cid)
                    continue
                veto: Dict[str, Any] = {}
                for _alias in _episode_id_aliases(cid):
                    if _TERMINAL_VETO_BY_EPISODE.get(_alias):
                        veto = dict(_TERMINAL_VETO_BY_EPISODE.get(_alias) or {})
                        break
                if veto:
                    # A4: stale BIRTH/refresh after terminal can never recreate
                    # the candidate even when the caller races with LivePulseSeal.
                    existing_terminal_target = _CANDIDATES.get(cid)
                    if existing_terminal_target is not None and not _is_terminal(existing_terminal_target):
                        terminalized = _apply_cross_layer_terminal(
                            existing_terminal_target, str(veto.get("terminal_state") or "LIVE_FAILED"),
                            row=veto, at=str(veto.get("terminal_at") or now.isoformat()),
                        )
                        _CANDIDATES[cid] = terminalized
                        candidate_audit_intents.append(("", dict(terminalized)))
                    continue
                if not _is_candidate_row(row):
                    continue
                probability_scope = str(row.get("probability_scope") or row.get("gann20_probability_scope") or "")
                episode_state = str(row.get("gann20_episode_state") or row.get("live_pulse_seal_state") or row.get("action_state") or "")
                is_sealed_update = bool(is_authoritative_sealed_probability(row))
                p50, p100 = _extract_probability(row, prefer_sealed=is_sealed_update)
                levels = _extract_levels(row, prefer_sealed=is_sealed_update)
                existing_state = dict(_CANDIDATES.get(cid) or {})
                state = dict(existing_state)
                is_new_candidate = not bool(existing_state)
                first_seen = state.get("first_seen_utc") or now.isoformat()
                # R162: keep execution-context fields in the tape.  P50>=30 is
                # a radar pulse; only the execution layer may call it money-ready.
                execution_context = {
                    k: row.get(k)
                    for k in (
                        "volume_ratio20", "relative_volume20", "rel_volume20", "rvol20",
                        "market_breadth_pos_pct", "market_breadth_pct", "price_breadth_pct",
                        "pbi_bar_pct", "pbi_value", "breadth_score", "market_breadth_pos",
                        "close_position", "close_location", "close_location_ratio",
                        "upper_wick_ratio", "upper_wick_to_range", "upper_wick_range_ratio", "upper_wick_pct",
                        "ret1_pct", "change_pct", "move_since_cross_pct", "ret8",
                        "pulse_gap_pct", "gap_pct", "rsiscaled_var_gap_pct",
                        "open", "high", "low", "close", "current_price", "appearance_price",
                        "atr", "atr14", "atr_value", "pulse_atr", "atr_at_cross",
                    )
                    if row.get(k) not in (None, "")
                }
                mutable = {
                    "version": VERSION,
                    "candidate_id": cid,
                    "market_key": market,
                    "market_slug": slug,
                    "symbol": symbol,
                    "name": str(row.get("name") or row.get("stock_name") or ""),
                    "recommendation_datetime": str(row.get("recommendation_datetime") or ""),
                    "source": source,
                    "first_seen_utc": first_seen,
                    "last_seen_utc": now.isoformat(),
                    "reason": str(row.get("reason") or row.get("var3_gann_category_text") or ""),
                    "radar_stage": str(row.get("radar_stage") or row.get("stock_rating") or row.get("action_state") or ""),
                    "is_internal_watch": bool((math.isfinite(p50) and DISCARD_BELOW_PCT <= p50 < _display_threshold_pct(market)) or "waiting_r1" in _text(row, "reason", "radar_stage").lower()),
                    "is_immediate_probability": bool(math.isfinite(p50) and p50 >= _display_threshold_pct(market)),
                    "gann20_contract_version": GANN20_CONTRACT_VERSION,
                    "model_horizon_bars": HORIZON_BARS,
                    "probability_scope_last": probability_scope or state.get("probability_scope_last"),
                    "episode_state": episode_state or state.get("episode_state"),
                    "sealed_truth_id": row.get("sealed_truth_id") or row.get("effective_truth_id") or state.get("sealed_truth_id"),
                    "effective_truth_id": row.get("effective_truth_id") or row.get("sealed_truth_id") or state.get("effective_truth_id"),
                    **_candidate_identity_fields(row, state),
                    "first_cross_at": row.get("first_cross_at") or state.get("first_cross_at"),
                    "first_cross_price": row.get("first_cross_price") if row.get("first_cross_price") not in (None, "") else state.get("first_cross_price"),
                    "birth_source_observation_id": row.get("birth_source_observation_id") or row.get("r1_watch_source_observation_id") or state.get("birth_source_observation_id"),
                    "r1_watch_mode": row.get("r1_watch_mode") or state.get("r1_watch_mode"),
                    "activated_r1": bool(state.get("activated_r1")),
                    "hit_r50": bool(state.get("hit_r50")),
                    "hit_r100": bool(state.get("hit_r100")),
                    "lost_r1": bool(state.get("lost_r1")),
                    **execution_context,
                }
                state.update(mutable)
                if is_new_candidate:
                    # R154: identity and model geometry are immutable for a candidate.
                    # Subsequent UI/radar refreshes may update only live fields above.
                    state.update({
                        "pulse_bar_time": str(row.get("first_cross_bar") or row.get("signal_bar_time") or row.get("pulse_bar_time") or row.get("signal_time") or row.get("gann_anchor_time") or row.get("recommendation_datetime") or ""),
                        "p50_at_first_cross": row.get("p50_at_first_cross") if row.get("p50_at_first_cross") not in (None, "") else p50,
                        "p100_at_first_cross": row.get("p100_at_first_cross") if row.get("p100_at_first_cross") not in (None, "") else p100,
                        "p50_live_first": p50,
                        "p100_live_first": p100,
                        "live_r1_frozen": levels.get("r1_frozen"),
                        "live_r50_frozen": levels.get("r50_frozen"),
                        "live_r100_frozen": levels.get("r100_frozen"),
                        "live_stop_frozen": levels.get("stop_frozen"),
                        "live_anchor_price": levels.get("pulse_anchor_price"),
                        "p50_frozen": p50,
                        "p100_frozen": p100,
                        "model_observed_bars": [],
                        "model_bars_elapsed": 0,
                        "model_bars_remaining": HORIZON_BARS,
                        "model_horizon_expired": False,
                        "r1_watch_armed": bool(row.get("r1_watch_armed")),
                        "atr_at_cross": next(
                            (
                                row.get(_atr_key)
                                for _atr_key in ("atr_at_cross", "atr", "atr14", "atr_value", "pulse_atr")
                                if row.get(_atr_key) not in (None, "")
                            ),
                            None,
                        ),
                        **levels,
                    })
                    # R159: initialize truth from the row that created the candidate.
                    # A candidate can be registered after R1/R50 already happened.
                    try:
                        price0 = _safe_float(row.get("appearance_price", row.get("current_price", row.get("close"))), math.nan)
                        high0 = _safe_float(row.get("signal_bar_high", row.get("high", row.get("current_high"))), math.nan)
                        low0 = _safe_float(row.get("signal_bar_low", row.get("low", row.get("current_low"))), math.nan)
                        if touch_level_state is not None:
                            # The registration row describes the cross bar itself.
                            # R1 is a *future-bar* activation after the exact seal; it
                            # must never be back-filled from the cross bar high.  Keep
                            # R50/R100 truth, but temporarily hide R1 from the generic
                            # level helper until a later source observation arrives.
                            _saved_r1 = state.get("r1_frozen")
                            state["r1_frozen"] = math.nan
                            try:
                                _changed0 = touch_level_state(state, price=price0, high=high0, low=low0, market_key=market, symbol=symbol, now=now.isoformat())
                            finally:
                                state["r1_frozen"] = _saved_r1
                            state["activated_r1"] = False
                            state.pop("r1_activation_tick_ts", None)
                            state.pop("r1_activation_price", None)
                            state["lost_r1"] = False
                            state.pop("lost_r1_tick_ts", None)
                            state.pop("lost_r1_price", None)
                            if state.get("hit_r50"):
                                state.setdefault("target_consumed_in_cross_bar", True)
                                state.setdefault("target_consumed_at", now.isoformat())
                                state.setdefault("target_consumed_reason", "R50_TOUCHED_IN_CROSS_BAR_BEFORE_R1_LANE_OPENED")
                            # The trained label starts *after* the sealed anchor bar.
                            # A cross-bar touch is important for no-chase/execution,
                            # but it is not an R50/R100 model outcome.  Keep separate
                            # audit flags and leave future-bar outcome truth open.
                            state["cross_bar_hit_r50"] = bool(state.get("hit_r50"))
                            state["cross_bar_hit_r100"] = bool(state.get("hit_r100"))
                            state["hit_r50"] = False
                            state["hit_r100"] = False
                            for _k in ("first_r50_tick_ts", "first_r50_price", "first_r100_tick_ts", "first_r100_price"):
                                state.pop(_k, None)
                    except Exception as exc:
                        _record_stage_error(
                            "event_write", "initialize_cross_bar_truth", exc,
                            market=market, symbol=symbol, episode_id=cid,
                            source_observation_id=str(row.get("source_observation_id") or ""),
                            reason_code="CROSS_BAR_TRUTH_INITIALIZATION_FAILED",
                        )
                _apply_candidate_probability_geometry(
                    state, row, levels, market=market, source=source, p50=p50, p100=p100,
                    is_sealed_update=is_sealed_update, probability_scope=probability_scope,
                    episode_state=episode_state, now=now,
                )

                # R162: first-pass execution decision at candidate registration.
                try:
                    if callable(_evaluate_execution_layer):
                        _exec_levels = {
                            "anchor_price": state.get("pulse_anchor_price"),
                            "r1_price": state.get("r1_frozen"),
                            "r50_price": state.get("r50_frozen"),
                            "r100_price": state.get("r100_frozen"),
                            "stop_price": state.get("stop_frozen"),
                        }
                        _exec_prob = {"p50_pct": state.get("p50_frozen"), "p100_pct": state.get("p100_frozen")}
                        _exec_result = _evaluate_execution_layer(market, state, _exec_prob, levels=_exec_levels)
                        state["execution_layer_result"] = _exec_result
                        state.update(_flatten_execution_result(_exec_result))
                except Exception as _exec_err:
                    state["execution_layer_error"] = str(_exec_err)[:240]
                state.setdefault("episode_id", cid)
                state.setdefault("pulse_episode_id", cid)
                state = _merge_candidate_truth(
                    existing_state, state, row, is_sealed_update=is_sealed_update,
                    stamped_at=now.isoformat(), producer_version=VERSION,
                )
                _CANDIDATES[cid] = state
                _CANDIDATE_IDS_BY_MARKET_SYMBOL.setdefault((market, symbol), set()).add(cid)
                # Candidate Store is the registration authority.  Candidate
                # JSONL is derived research telemetry: queue it only after the
                # durable candidate snapshot succeeds and never let audit I/O
                # change the registration receipt.
                sig = json.dumps({k: state.get(k) for k in ("candidate_id", "symbol", "r1_frozen", "r50_frozen", "r100_frozen", "p50_frozen", "pulse_bar_time", "r1_watch_armed", "probability_scope_last")}, ensure_ascii=False, sort_keys=True)
                if sig not in _SEEN_CANDIDATE_WRITES and sig not in _RESEARCH_TELEMETRY_PENDING_SIGNATURES:
                    candidate_audit_intents.append((sig, dict(state)))
                registered += 1
                registered_ids.append(cid)
            except Exception as exc:
                raw_row = dict(raw or {}) if isinstance(raw, dict) else {}
                _record_stage_error(
                    "event_write", "register_pulse_candidate", exc,
                    market=market, symbol=str(raw_row.get("symbol") or ""),
                    episode_id=str(raw_row.get("pulse_episode_id") or raw_row.get("episode_id") or ""),
                    source_observation_id=str(raw_row.get("source_observation_id") or ""),
                    reason_code="PULSE_CANDIDATE_REGISTER_FAILED",
                )
                continue
        # The configured limit is a pressure watermark, never a deletion policy.
        # No active or outbox-bearing candidate may be evicted silently.
        capacity_overflow = max(0, len(_CANDIDATES) - _max_candidates())
        if capacity_overflow:
            _record_stage_error(
                "event_write", "pulse_candidate_capacity_pressure",
                RuntimeError(f"ACTIVE_CANDIDATE_CAPACITY_EXCEEDED:{len(_CANDIDATES)}>{_max_candidates()}"),
                market=market, reason_code="PULSE_CANDIDATE_CAPACITY_PRESSURE_NO_EVICTION",
            )
        persist_receipt = _persist_candidates_safely("persist_pulse_candidates_after_register", market)
    candidate_audit_queued = 0
    if bool(persist_receipt.get("ok")):
        for sig, audit_row in candidate_audit_intents:
            receipt = _enqueue_research_jsonl(cand_path, audit_row, kind="candidate_audit", signature=sig)
            if receipt.get("queued") or receipt.get("duplicate"):
                candidate_audit_queued += 1
    committed_ids = [cid for cid in registered_ids if cid in _CANDIDATES]
    return {
        "enabled": True, "registered": registered, "active": len(_CANDIDATES), "version": VERSION,
        "candidate_ids": committed_ids, "persisted": bool(persist_receipt.get("ok")),
        "persisted_candidates": int(persist_receipt.get("persisted_candidates") or 0),
        "candidate_store_path": persist_receipt.get("path"), "capacity_overflow": capacity_overflow,
        "commit_receipt": bool(committed_ids and persist_receipt.get("ok")),
        "candidate_audit_authoritative": False,
        "candidate_audit_queued": int(candidate_audit_queued),
    }




def _bar_dt(value: Any, market_key: str = "") -> Optional[_dt.datetime]:
    try:
        return _market_to_naive(value, market_key=market_key or "local_السوق السعودي")
    except Exception:
        return None


def _timeframe_minutes(value: Any) -> int:
    text = str(value or "").strip().upper().replace(" ", "")
    match = re.fullmatch(r"(\d+)(M|MIN|MINS|MINUTE|MINUTES|H|HR|HRS|HOUR|HOURS)", text)
    if match:
        amount = int(match.group(1) or 0)
        unit = str(match.group(2) or "")
        return amount * 60 if unit.startswith("H") else amount
    # Accept common provider aliases without broadening Episode identity rules:
    # this parser is used only for bounded 20-bar outcome membership.
    prefix = re.fullmatch(r"(M|MIN|H|HR)(\d+)", text)
    if prefix:
        unit = str(prefix.group(1) or "")
        amount = int(prefix.group(2) or 0)
        return amount * 60 if unit.startswith("H") else amount
    return 0


def _canonical_bar_key(value: Any, market_key: str = "", timeframe: Any = "") -> str:
    dt_value = _bar_dt(value, market_key=market_key)
    if dt_value is None:
        return str(value or "").strip()
    minutes = _timeframe_minutes(timeframe)
    if 0 < minutes <= 24 * 60:
        minute_of_day = dt_value.hour * 60 + dt_value.minute
        bucket = (minute_of_day // minutes) * minutes
        dt_value = dt_value.replace(
            hour=(bucket // 60) % 24, minute=bucket % 60, second=0, microsecond=0,
        )
    return dt_value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")


def _normalize_candidate_observed_bars(candidate: Dict[str, Any], *, market_key: str = "", timeframe: Any = "") -> List[str]:
    market = str(market_key or candidate.get("market_key") or "")
    frame = str(timeframe or candidate.get("timeframe") or candidate.get("model_timeframe") or "30M")
    unique: Dict[str, _dt.datetime] = {}
    for raw in list(candidate.get("model_observed_bars") or []):
        key = _canonical_bar_key(raw, market_key=market, timeframe=frame)
        if not key:
            continue
        unique[key] = _bar_dt(key, market_key=market) or _dt.datetime.max
    ordered = sorted(unique, key=lambda key: unique[key])[-HORIZON_BARS:]
    candidate["model_observed_bars"] = ordered
    candidate["model_bars_elapsed"] = min(HORIZON_BARS, len(ordered))
    candidate["model_bars_remaining"] = max(0, HORIZON_BARS - len(ordered))
    return ordered


def _is_future_bar(current_bar: Any, anchor_bar: Any, *, market_key: str = "") -> bool:
    current_dt = _bar_dt(current_bar, market_key=market_key)
    anchor_dt = _bar_dt(anchor_bar, market_key=market_key)
    if current_dt is not None and anchor_dt is not None:
        return current_dt > anchor_dt
    current_text = str(current_bar or "").strip()
    anchor_text = str(anchor_bar or "").strip()
    return bool(current_text and anchor_text and current_text != anchor_text and current_text > anchor_text)

def _extract_price_payload(raw: Any) -> Tuple[float, Optional[float], str, Dict[str, Any]]:
    if isinstance(raw, dict):
        price = _safe_float(raw.get("current_price", raw.get("close", raw.get("price", raw.get("last", raw.get("tick_price"))))), math.nan)
        change = _safe_float(raw.get("change_pct", raw.get("change", raw.get("ret1_pct"))), math.nan)
        ts = str(raw.get("timestamp") or raw.get("last_update") or raw.get("data_datetime") or raw.get("bar_time") or "")
        return price, (change if math.isfinite(change) else None), ts, dict(raw)
    price = _safe_float(raw, math.nan)
    return price, None, "", {}


def _evaluate_r1_tick_truth(
    cand: Dict[str, Any], raw: Dict[str, Any], ctx: Dict[str, Any], *, source: str,
    market: str, price: float, future_observation: bool, model_open: bool, now: _dt.datetime,
) -> Dict[str, Any]:
    """Evaluate one immutable source observation; cumulative candle high never births R1."""
    source_is_replay = str(source or "").lower().startswith("replay") or bool(ctx.get("historical_replay"))
    threshold = float(_display_threshold_pct(market))
    source_id = str(raw.get("source_observation_id") or ctx.get("source_observation_id") or "")
    birth_source_id = str(cand.get("birth_source_observation_id") or "")
    later_source = bool(source_id and birth_source_id and source_id != birth_source_id)
    mode = str(cand.get("r1_watch_mode") or "").upper()
    p50_birth = _safe_float(cand.get("p50_at_first_cross", cand.get("p50_frozen")), math.nan)
    p50_sealed = _safe_float(cand.get("p50_sealed"), math.nan)
    state_now = str(cand.get("episode_state") or "").upper()
    post_r1 = bool(_is_post_r1(cand))
    live_watch = bool(
        cand.get("r1_watch_armed") and mode == "LIVE_BIRTH_R1"
        and (state_now == "LIVE_WAITING_R1" or post_r1)
        and math.isfinite(p50_birth) and DISCARD_BELOW_PCT <= p50_birth < threshold
        and later_source and not bool(cand.get("historical_reconstructed")) and not source_is_replay
    )
    sealed_watch = bool(
        cand.get("r1_watch_armed") and mode == "SEALED_R1"
        and (state_now == "SEALED_WAITING_R1" or post_r1)
        and math.isfinite(p50_sealed) and DISCARD_BELOW_PCT <= p50_sealed < threshold
        and future_observation and not bool(cand.get("historical_reconstructed")) and not source_is_replay
    )
    watch_armed = bool(live_watch or sealed_watch)
    prefix = "live" if live_watch else "sealed"
    r1 = _safe_float(cand.get(f"{prefix}_r1_frozen") if watch_armed else math.nan, math.nan)
    r50 = _safe_float(cand.get(f"{prefix}_r50_frozen", cand.get("live_r50_frozen")), math.nan)
    r100 = _safe_float(cand.get(f"{prefix}_r100_frozen", cand.get("live_r100_frozen")), math.nan)
    stop = _safe_float(cand.get(f"{prefix}_stop_frozen", cand.get("stop_frozen")), math.nan)
    tick_high = _safe_float(raw.get("high"), math.nan)
    tick_low = _safe_float(raw.get("low"), math.nan)
    eligible = bool(model_open and watch_armed and (later_source if live_watch else future_observation))
    transitions = _apply_r1_tick_transitions(
        cand, r1=r1, r50=r50, r100=r100, stop=stop, eligible=eligible,
        watch_armed=watch_armed, live_watch=live_watch, future_observation=future_observation,
        model_open=model_open, price=price, at=now.isoformat(), source_observation_id=source_id,
    )
    crossed_r1, hit_r50, hit_r100 = (
        transitions["crossed_r1"], transitions["hit_r50"], transitions["hit_r100"]
    )
    cross_bar_r50, lost_r1 = transitions["cross_bar_r50"], transitions["lost_r1"]
    hit_stop, regained_r1 = transitions["hit_stop"], transitions["regained_r1"]
    target_consumed_was = transitions["target_consumed_was"]
    pre_entry_target_consumed = transitions["pre_entry_target_consumed"]

    pre_entry_stop_invalidated_now = _should_invalidate_pre_entry_stop(cand, hit_stop_now=hit_stop)
    if pre_entry_stop_invalidated_now:
        _apply_pre_entry_stop_invalidation(
            cand, at=now.isoformat(), price=price, source_observation_id=source_id,
        )
        live_watch = sealed_watch = watch_armed = False
        crossed_r1 = hit_r50 = hit_r100 = lost_r1 = regained_r1 = False
    return {
        "source_is_replay": source_is_replay, "source_observation_id": source_id,
        "birth_source_observation_id": birth_source_id, "later_source_observation": later_source,
        "p50_live_birth": p50_birth, "p50_sealed_now": p50_sealed,
        "live_r1_watch": live_watch, "sealed_r1_watch": sealed_watch, "r1_watch_armed": watch_armed,
        "r1": r1, "r50": r50, "r100": r100, "stop": stop, "tick_high": tick_high, "tick_low": tick_low,
        "crossed_r1_now": crossed_r1, "hit_r50_now": hit_r50, "hit_r100_now": hit_r100,
        "cross_bar_r50_now": cross_bar_r50, "lost_r1_now": lost_r1, "hit_stop_now": hit_stop,
        "regained_r1_now": regained_r1, "target_consumed_was": target_consumed_was,
        "pre_entry_target_consumed_now": pre_entry_target_consumed,
        "pre_entry_stop_invalidated_now": pre_entry_stop_invalidated_now,
    }


def record_price_batch(market_key: str, prices_map: Dict[str, Any], source: str = "price_patch", context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not _enabled() or not prices_map:
        return {"enabled": bool(_enabled()), "written": 0}
    market = str(market_key or "")
    slug = _market_slug(market)
    now = _now_utc()
    tick_path = _base_dir() / f"{slug}_pulse_ticks.jsonl"
    written = matched_candidates = 0
    observation_accepted_ids: List[str] = []; live_event_rows: List[Dict[str, Any]] = []
    research_event_rows: List[Dict[str, Any]] = []
    tick_audit_intents: List[Dict[str, Any]] = []
    # Truth-ledger events are staged until candidate state persistence succeeds.
    # This prevents a failed candidate save from leaving a durable-looking R1/
    # target/terminal event that the candidate snapshot itself cannot restore.
    episode_event_intents: List[Tuple[str, Dict[str, Any], Dict[str, Any], bool]] = []
    immediate_terminal_candidate_ids: List[str] = []
    projection_debt_errors: List[str] = []
    ctx = dict(context or {})
    min_interval = _min_tick_interval_sec()
    with _LOCK:
        _ensure_candidates_restored_locked()
        _restore_terminal_veto_locked()
        # Persistence failure must not leave a non-durable lifecycle mutation in
        # memory where it can be published or suppress the exact retry. Snapshot
        # only the symbols touched by this batch; terminal SQLite remains the
        # higher-rank authority and is re-applied during rollback when needed.
        touched_symbols = {str(sym or "").strip().upper() for sym in (prices_map or {}) if str(sym or "").strip()}
        rollback_candidate_ids = {
            cid for sym in touched_symbols
            for cid in tuple(_CANDIDATE_IDS_BY_MARKET_SYMBOL.get((market, sym), set()))
            if cid in _CANDIDATES
        }
        rollback_candidates = {cid: copy.deepcopy(_CANDIDATES[cid]) for cid in rollback_candidate_ids}
        rollback_last_ticks = {
            (market, sym): (
                (market, sym) in _LAST_TICK_BY_SYMBOL,
                copy.deepcopy(_LAST_TICK_BY_SYMBOL.get((market, sym))),
            )
            for sym in touched_symbols
        }
        recovery_reconciled = False
        for raw_symbol, raw_payload in (prices_map or {}).items():
            sym = str(raw_symbol or "").strip().upper()
            candidate_ids = tuple(_CANDIDATE_IDS_BY_MARKET_SYMBOL.get((market, sym), set()))
            candidates_for_symbol = [
                _CANDIDATES[cid] for cid in candidate_ids if cid in _CANDIDATES
            ]
            if not sym or not candidates_for_symbol:
                continue
            price, change_pct, ts_text, raw = _extract_price_payload(raw_payload)
            if not math.isfinite(price) or price <= 0:
                continue
            last_key = (market, sym)
            last_price, last_ts = _LAST_TICK_BY_SYMBOL.get(last_key, (math.nan, 0.0))
            now_monotonic = time.monotonic()
            if math.isfinite(last_price) and abs(last_price - price) < 1e-12 and (now_monotonic - last_ts) < min_interval:
                continue
            _LAST_TICK_BY_SYMBOL[last_key] = (price, now_monotonic)
            for cand in candidates_for_symbol:
                cid = str(cand.get("candidate_id") or cand.get("episode_id") or "").strip()
                veto = _terminal_veto_for_episode_locked(cid)
                if veto and not _is_terminal(cand):
                    _apply_cross_layer_terminal(
                        cand, str(veto.get("terminal_state") or "LIVE_FAILED"),
                        row=veto, at=str(veto.get("terminal_at") or now.isoformat()),
                    )
                    cand["runtime_terminal_veto_dominated_candidate_state"] = True
                    recovery_reconciled = True
                if _is_terminal(cand):
                    continue
                matched_candidates += 1
                observation_accepted_ids.append(str(cand.get("candidate_id") or cand.get("episode_id") or ""))
                current_bar_time = str(raw.get("bar_time") or raw.get("data_datetime") or ts_text or ctx.get("bar_time") or "")
                anchor_bar = str(cand.get("pulse_bar_time") or "")
                model_timeframe = str(cand.get("timeframe") or cand.get("model_timeframe") or "30M")
                observed_bars = _normalize_candidate_observed_bars(
                    cand, market_key=market, timeframe=model_timeframe,
                )
                current_bar_key = _canonical_bar_key(
                    current_bar_time, market_key=market, timeframe=model_timeframe,
                )
                anchor_bar_key = _canonical_bar_key(
                    anchor_bar, market_key=market, timeframe=model_timeframe,
                )
                future_observation = _is_future_bar(current_bar_key, anchor_bar_key, market_key=market)
                if future_observation and current_bar_key and current_bar_key not in observed_bars:
                    observed_bars.append(current_bar_key)
                    observed_bars.sort(key=lambda x: _bar_dt(x, market_key=market) or _dt.datetime.max)
                    cand["model_observed_bars"] = observed_bars[-HORIZON_BARS:]
                cand["model_bars_elapsed"] = min(HORIZON_BARS, len(cand.get("model_observed_bars") or []))
                cand["model_bars_remaining"] = max(0, HORIZON_BARS - int(cand.get("model_bars_elapsed") or 0))
                was_horizon_expired = bool(cand.get("model_horizon_expired"))
                cand["model_horizon_expired"] = bool(int(cand.get("model_bars_elapsed") or 0) >= HORIZON_BARS)
                horizon_expired_now = bool(cand.get("model_horizon_expired") and not was_horizon_expired)
                negative_cross_now = bool(raw.get("negative_cross_now")) and not bool(cand.get("model_episode_closed_by_negative_cross"))
                if bool(raw.get("negative_cross_now")):
                    cand["model_episode_closed_by_negative_cross"] = True
                    cand.setdefault("negative_cross_closed_at", now.isoformat())
                model_open = not bool(cand.get("model_horizon_expired") or cand.get("model_episode_closed_by_negative_cross"))
                previous_episode_state = str(cand.get("episode_state") or "")
                tick_truth = _evaluate_r1_tick_truth(
                    cand, raw, ctx, source=source, market=market, price=price,
                    future_observation=future_observation, model_open=model_open, now=now,
                )
                _source_is_replay = tick_truth["source_is_replay"]
                source_observation_id = tick_truth["source_observation_id"]
                birth_source_observation_id = tick_truth["birth_source_observation_id"]
                later_source_observation = tick_truth["later_source_observation"]
                p50_live_birth, p50_sealed_now = tick_truth["p50_live_birth"], tick_truth["p50_sealed_now"]
                live_r1_watch, sealed_r1_watch = tick_truth["live_r1_watch"], tick_truth["sealed_r1_watch"]
                r1_watch_armed = tick_truth["r1_watch_armed"]
                r1, r50, r100, stop = tick_truth["r1"], tick_truth["r50"], tick_truth["r100"], tick_truth["stop"]
                tick_high, tick_low = tick_truth["tick_high"], tick_truth["tick_low"]
                crossed_r1_now, hit_r50_now = tick_truth["crossed_r1_now"], tick_truth["hit_r50_now"]
                hit_r100_now, lost_r1_now = tick_truth["hit_r100_now"], tick_truth["lost_r1_now"]
                hit_stop_now, regained_r1_now = tick_truth["hit_stop_now"], tick_truth["regained_r1_now"]
                target_consumed_was = tick_truth["target_consumed_was"]
                pre_entry_stop_invalidated_now = bool(tick_truth.get("pre_entry_stop_invalidated_now"))
                if hit_stop_now and bool(cand.get("activated_r1")):
                    cand["stopped_out_at"] = now.isoformat()
                    cand["stopped_out_price"] = float(tick_low if math.isfinite(tick_low) else price)
                    cand["exit_price"] = cand["stopped_out_price"]
                    cand["exit_reason"] = "STOPPED_OUT"
                episode_state_now, presentation_now, lifecycle_notification = _transition_lifecycle(
                    cand, previous_state=previous_episode_state, now=now, crossed_r1=crossed_r1_now, lost_r1=lost_r1_now,
                    regained_r1=regained_r1_now, hit_r50=hit_r50_now, hit_r100=hit_r100_now, hit_stop=hit_stop_now)
                if lifecycle_notification and str(episode_state_now or "").upper() in DURABLE_TERMINAL_STATES:
                    _terminal_cid = str(cand.get("candidate_id") or cand.get("episode_id") or "").strip()
                    if _terminal_cid and _terminal_cid not in immediate_terminal_candidate_ids:
                        immediate_terminal_candidate_ids.append(_terminal_cid)
                p50 = _safe_float(cand.get("p50_frozen"), math.nan)
                p100 = _safe_float(cand.get("p100_frozen"), math.nan)
                execution_result = {}
                execution_flat = {}
                try:
                    if callable(_evaluate_execution_layer):
                        exec_obs = dict(cand)
                        exec_obs.update({
                            "current_price": price,
                            "close": price,
                            "price": price,
                            "high": tick_high if math.isfinite(tick_high) else price,
                            "low": tick_low if math.isfinite(tick_low) else price,
                            "r50_price": r50,
                            "r100_price": r100,
                            "stop_price": cand.get("stop_frozen"),
                            "anchor_price": cand.get("pulse_anchor_price"),
                        })
                        execution_result = _evaluate_execution_layer(
                            market, exec_obs, {"p50_pct": p50, "p100_pct": p100},
                            levels={
                                "anchor_price": cand.get("pulse_anchor_price"),
                                "r1_price": r1,
                                "r50_price": r50,
                                "r100_price": r100,
                                "stop_price": cand.get("stop_frozen"),
                            },
                            paac_row=cand,
                        )
                        execution_flat = _flatten_execution_result(execution_result)
                except Exception as _exec_tick_err:
                    _record_stage_error(
                        "event_write", "evaluate_tick_execution_shadow", _exec_tick_err,
                        market=market, symbol=sym, episode_id=str(cand.get("candidate_id") or ""),
                        source_observation_id=str(raw.get("source_observation_id") or ctx.get("source_observation_id") or ""),
                        reason_code="TICK_EXECUTION_SHADOW_EVALUATION_FAILED",
                    )
                    execution_result = {"execution_decision": "EXECUTION_LAYER_ERROR", "execution_passed": False, "execution_blockers": [str(_exec_tick_err)[:120]]}
                    execution_flat = _flatten_execution_result(execution_result) if callable(_flatten_execution_result) else {}
                execution_shadow_passed_now = bool((execution_result or {}).get("execution_shadow_passed", (execution_result or {}).get("execution_passed"))) and not bool(cand.get("target_consumed_before_entry"))
                execution_authorized_now = bool((execution_result or {}).get("execution_authorized"))
                execution_passed_now = bool(execution_authorized_now and (execution_result or {}).get("execution_passed")) and not bool(cand.get("target_consumed_before_entry"))
                if cand.get("pre_entry_stop_invalidated"):
                    model_verdict = "pre_entry_stop_invalidated"
                elif cand.get("model_episode_closed_by_negative_cross"):
                    model_verdict = "episode_closed_negative_cross"
                elif cand.get("model_horizon_expired"):
                    model_verdict = "episode_expired_20_bars"
                elif cand.get("target_consumed_before_entry"):
                    model_verdict = "target_consumed_before_entry"
                elif execution_passed_now:
                    model_verdict = "execution_authorized_candidate"
                elif execution_shadow_passed_now:
                    model_verdict = "r161_shadow_candidate"
                elif math.isfinite(p50) and p50 >= _display_threshold_pct(market):
                    model_verdict = "p30_radar_only"
                elif r1_watch_armed and math.isfinite(p50) and p50 >= DISCARD_BELOW_PCT and (cand.get("activated_r1") or crossed_r1_now):
                    model_verdict = "r1_watch_activation_not_execution"
                elif r1_watch_armed and math.isfinite(p50) and p50 >= DISCARD_BELOW_PCT:
                    model_verdict = "internal_waiting_r1"
                else:
                    model_verdict = "watch_only"
                official_verdict = (
                    "أُبطلت المراقبة — ضُرب الوقف قبل الدخول" if model_verdict == "pre_entry_stop_invalidated"
                    else "انتهت الحلقة بالتقاطع السلبي" if model_verdict == "episode_closed_negative_cross"
                    else "انتهى أفق GANN20 بعد 20 شمعة" if model_verdict == "episode_expired_20_bars"
                    else "هدف مستهلك قبل الدخول" if model_verdict == "target_consumed_before_entry"
                    else "تنفيذ معتمد" if model_verdict == "execution_authorized_candidate"
                    else "مرشح R161 ظل — لا شراء" if model_verdict == "r161_shadow_candidate"
                    else "رادار P30 فقط — لا شراء" if model_verdict == "p30_radar_only"
                    else "اختراق مراقبة R1 — غير تنفيذي" if model_verdict == "r1_watch_activation_not_execution"
                    else "مراقبة داخلية"
                )
                payload = {
                    "version": VERSION,
                    "record_ts": now.isoformat(),
                    "market": market,
                    "market_key": market,
                    "decision_market_key": market,
                    "decision_lane": "radar",
                    "market_slug": slug,
                    "symbol": sym,
                    "name": cand.get("name") or sym,
                    "candidate_id": cand.get("candidate_id"),
                    "pulse_episode_id": cand.get("candidate_id"),
                    "id": cand.get("candidate_id"),
                    "sealed_truth_id": cand.get("sealed_truth_id") or cand.get("effective_truth_id"),
                    "effective_truth_id": cand.get("effective_truth_id") or cand.get("sealed_truth_id"),
                    "pulse_bar_time": cand.get("pulse_bar_time"),
                    "signal_bar_time": cand.get("pulse_bar_time"),
                    "first_cross_bar": cand.get("pulse_bar_time"),
                    "first_cross_at": cand.get("first_cross_at"),
                    "first_cross_price": cand.get("first_cross_price"),
                    "atr_at_cross": cand.get("atr_at_cross"),
                    "current_bar_time": current_bar_time,
                    "tick_price": price,
                    "current_price": price,
                    "close": price,
                    "appearance_price": price if crossed_r1_now else cand.get("appearance_price"),
                    "recommendation_datetime": cand.get("first_r1_tick_ts") if crossed_r1_now else cand.get("recommendation_datetime"),
                    "change_pct": change_pct,
                    "tick_high": tick_high,
                    "tick_low": tick_low,
                    "tick_volume": _safe_float(raw.get("volume"), math.nan),
                    "source": source,
                    "pulse_anchor_price": cand.get("live_anchor_price") if live_r1_watch else cand.get("sealed_anchor_price", cand.get("pulse_anchor_price")),
                    **_r1_event_truth(
                        cand, live_r1_watch=live_r1_watch, sealed_r1_watch=sealed_r1_watch,
                        live_scope=PROB_SCOPE_LIVE_CURRENT_BAR, sealed_scope=PROB_SCOPE_SEALED_CROSS_BAR,
                    ),
                    "p50_frozen": cand.get("p50_frozen"),
                    "p100_frozen": cand.get("p100_frozen"),
                    "r1_frozen": r1,
                    "r50_frozen": r50,
                    "r100_frozen": r100,
                    "stop_frozen": cand.get("stop_frozen"),
                    "gann_r1_breakout_point": r1,
                    "gann_r3_resistance_50": r50,
                    "gann_r5_resistance_100": r100,
                    "gann_pivot_stop_loss": cand.get("stop_frozen"),
                    "gann_anchor_price": cand.get("pulse_anchor_price"),
                    "gann20_contract_version": GANN20_CONTRACT_VERSION,
                    "model_horizon_bars": HORIZON_BARS,
                    "model_bars_elapsed": cand.get("model_bars_elapsed"),
                    "model_bars_remaining": cand.get("model_bars_remaining"),
                    "model_horizon_expired": bool(cand.get("model_horizon_expired")),
                    "model_episode_closed_by_negative_cross": bool(cand.get("model_episode_closed_by_negative_cross")),
                    "r1_watch_armed": r1_watch_armed,
                    "r1_watch_mode": "LIVE_BIRTH_R1" if live_r1_watch else "SEALED_R1" if sealed_r1_watch else None,
                    "r1_levels_source": "LIVE_BIRTH_OBSERVATION" if live_r1_watch else "SEALED_CROSS_BAR" if sealed_r1_watch else "NOT_ARMED",
                    "birth_source_observation_id": birth_source_observation_id or None,
                    "source_observation_id": source_observation_id or None,
                    "p50_at_first_cross": cand.get("p50_at_first_cross"),
                    "p100_at_first_cross": cand.get("p100_at_first_cross"),
                    "gann20_episode_state": episode_state_now, "live_pulse_seal_state": episode_state_now, "action_state": episode_state_now,
                    "gann20_episode_outcome": episode_state_now if _is_post_r1(cand) else None, "gann20_episode_outcome_ar": presentation_now.get("status") if _is_post_r1(cand) else None,
                    "sealed_model_anchor_at": cand.get("sealed_model_anchor_at"),
                    "sealed_model_anchor_price": cand.get("sealed_model_anchor_price"),
                    "is_internal_watch": cand.get("is_internal_watch"),
                    "is_immediate_probability": cand.get("is_immediate_probability"),
                    "crossed_r1_now": crossed_r1_now,
                    "hit_r50_now": hit_r50_now,
                    "hit_r100_now": hit_r100_now,
                    "lost_r1_now": lost_r1_now,
                    "regained_r1_now": regained_r1_now,
                    "hit_stop_now": hit_stop_now,
                    "pre_entry_stop_invalidated": bool(cand.get("pre_entry_stop_invalidated")),
                    "pre_entry_stop_invalidated_at": cand.get("pre_entry_stop_invalidated_at"),
                    "terminal_state": PRE_ENTRY_STOP_INVALIDATED if cand.get("pre_entry_stop_invalidated") else cand.get("terminal_state"),
                    "activated_r1": bool(cand.get("activated_r1")),
                    "hit_r50": bool(cand.get("hit_r50")),
                    "hit_r100": bool(cand.get("hit_r100")),
                    "lost_r1": bool(cand.get("lost_r1")),
                    "target_consumed_before_entry": bool(cand.get("target_consumed_before_entry")),
                    "target_consumed_at": cand.get("target_consumed_at"),
                    "target_consumed_reason": cand.get("target_consumed_reason"),
                    "first_r1_tick_ts": cand.get("first_r1_tick_ts"),
                    "first_r1_crossed_at": cand.get("first_r1_tick_ts"),
                    "first_r1_price": cand.get("first_r1_price"),
                    "signal_entry_price": price if crossed_r1_now else None,
                    "entry_signal_price": price if crossed_r1_now else None,
                    "r1_future_bar_cross": bool(crossed_r1_now and future_observation),
                    "r1_same_bar_later_observation": bool(crossed_r1_now and live_r1_watch and not future_observation),
                    "live_sniper_contract_version": LIVE_SNIPER_CONTRACT_VERSION,
                    "live_sniper_event_type": LIVE_R1_BORN if crossed_r1_now else None,
                    "live_sniper_event_origin": ORIGIN_LIVE_PRICE_TICK if crossed_r1_now else (ORIGIN_HISTORICAL_REBUILD if _source_is_replay else None),
                    "live_sniper_birth_proven": bool(crossed_r1_now and not _source_is_replay),
                    "live_sniper_r1_birth_proven": bool(crossed_r1_now and not _source_is_replay),
                    "live_sniper_birth_kind": "R1" if crossed_r1_now else None,
                    "live_sniper_born_at": cand.get("first_r1_tick_ts") if crossed_r1_now else None,
                    "trader_board_session_date": str(cand.get("first_r1_tick_ts") or "")[:10] if crossed_r1_now else None,
                    "first_r50_tick_ts": cand.get("first_r50_tick_ts"),
                    "first_r100_tick_ts": cand.get("first_r100_tick_ts"),
                    "lost_r1_tick_ts": cand.get("lost_r1_tick_ts"),
                    "model_verdict_now": model_verdict,
                    "official_verdict_now": official_verdict,
                    "visible_signal_case_ar": "اختراق R1 حي" if crossed_r1_now else presentation_now.get("status"),
                    "radar_stage": presentation_now.get("radar_stage") or cand.get("radar_stage"), "status": presentation_now.get("status"),
                    "live_publishable": bool(presentation_now.get("live_publishable") and not cand.get("pre_entry_stop_invalidated")), "published_to_trader": bool(presentation_now.get("published_to_trader") and not cand.get("pre_entry_stop_invalidated")),
                    "execution_layer_version": EXECUTION_LAYER_VERSION,
                    "execution_decision_now": (execution_result or {}).get("execution_decision"),
                    "execution_passed_now": bool(execution_passed_now),
                    "execution_shadow_passed_now": bool(execution_shadow_passed_now),
                    "execution_authorized_now": bool(execution_authorized_now),
                    "execution_authority_now": (execution_result or {}).get("execution_authority") or "SHADOW_ONLY",
                    "execution_tier_now": (execution_result or {}).get("execution_tier") or (execution_result or {}).get("execution_profile"),
                    "execution_blockers_now": (execution_result or {}).get("execution_blockers"),
                    **execution_flat,
                }
                payload = _stamp_episode_truth(
                    payload,
                    truth_source=_candidate_truth_source(cand),
                    producer_contract_version=VERSION,
                )
                # Pulse tick JSONL is research telemetry only.  Stage it here
                # and queue it after Candidate/debt durability is known; its I/O
                # must never gate R1 truth, UI, or Source ACK.
                tick_audit_intents.append(dict(payload))
                def _research_event(event_type: str, reason_code: str = "") -> None:
                    event_row = {
                        **dict(payload),
                        "research_event_type": event_type,
                        "event_type": event_type,
                        "event_time": now.isoformat(),
                        "previous_state": previous_episode_state, "new_state": episode_state_now,
                        "reason_code": reason_code or event_type,
                        "source_observation_id": raw.get("source_observation_id") or ctx.get("source_observation_id"),
                    }
                    research_event_rows.append(_r16933_lifecycle_ui_patch(event_row))

                if pre_entry_stop_invalidated_now:
                    episode_event_intents.append((
                        PRE_ENTRY_STOP_INVALIDATED, dict(payload),
                        {"event_origin": ORIGIN_LIVE_PRICE_TICK, "reason_code": PRE_ENTRY_STOP_INVALIDATED}, True,
                    ))
                    _research_event(PRE_ENTRY_STOP_INVALIDATED, PRE_ENTRY_STOP_INVALIDATED)
                if crossed_r1_now and not _source_is_replay and not pre_entry_stop_invalidated_now:
                    transition_id = str((lifecycle_notification or {}).get("transition_id") or "")
                    episode_id = str(cand.get("episode_id") or cand.get("candidate_id") or "")
                    projection_id = _projection_id_for_transition(
                        episode_id, LIVE_R1_BORN, transition_id,
                    )
                    projection_payload = dict(payload)
                    projection_payload["projection_id"] = projection_id
                    projection = _add_lifecycle_projection(
                        cand,
                        transition_id,
                        projection_id=projection_id,
                        projection_type="GANN20_EPISODE_LEDGER",
                        event_type=LIVE_R1_BORN,
                        payload=projection_payload,
                        extra={"event_origin": ORIGIN_LIVE_PRICE_TICK, "projection_id": projection_id},
                        suppress_duplicate=True,
                        at=now,
                    ) if transition_id else None
                    if projection is None:
                        projection_debt_errors.append(
                            f"R1_PROJECTION_DEBT_NOT_ATTACHED:{episode_id}:{transition_id or 'missing_transition_id'}"
                        )
                    else:
                        payload["r1_projection_id"] = projection_id
                        cand["r1_birth_projection_id"] = projection_id
                    live_event_rows.append(dict(payload))
                    _research_event("R1_REACHED")
                if hit_r50_now:
                    episode_event_intents.append(("R50_HIT", dict(payload), {}, True))
                    _research_event("R50_REACHED")
                if hit_r100_now:
                    episode_event_intents.append(("R100_HIT", dict(payload), {}, True))
                    _research_event("R100_REACHED")
                if hit_stop_now and not pre_entry_stop_invalidated_now:
                    _research_event("STOP_REACHED")
                if lost_r1_now:
                    _research_event("CROSS_LOST", "R1_LOST_AFTER_ACTIVATION")
                if regained_r1_now:
                    _research_event("CROSS_REGAINED", "R1_REGAINED_AFTER_LOSS")
                if bool(cand.get("target_consumed_before_entry")) and not target_consumed_was:
                    _research_event("TARGET_CONSUMED", str(cand.get("target_consumed_reason") or "TARGET_CONSUMED"))
                if horizon_expired_now:
                    episode_event_intents.append(("EPISODE_EXPIRED_20_BARS", dict(payload), {}, True))
                    _research_event("EPISODE_EXPIRED", "MODEL_HORIZON_20_BARS")
                if negative_cross_now:
                    episode_event_intents.append(("EPISODE_CLOSED_NEGATIVE_CROSS", dict(payload), {}, True))
                    _research_event("NEGATIVE_CROSS", "EPISODE_CLOSED_NEGATIVE_CROSS")
                written += 1
        if projection_debt_errors:
            persist_receipt = {
                "ok": False,
                "durable": False,
                "persisted_candidates": 0,
                "path": str(_candidate_store_path(_candidate_state_root())),
                "error": ";".join(projection_debt_errors)[:1000],
                "projection_debt_required_but_missing": True,
            }
        elif matched_candidates > 0 or recovery_reconciled:
            # A matched active episode may have advanced bars, R1/target/stop truth,
            # or its lifecycle outbox.  A veto reconciliation is also a truth
            # mutation and must repair the lower-rank candidate snapshot durably.
            persist_receipt = _persist_candidates_safely("persist_pulse_candidates_after_tick", market)
        else:
            # PriceTape invokes this hook for every source observation.  Rewriting
            # the complete candidate store when the symbol has no active episode is
            # pure disk/Defender contention and cannot add truth.
            persist_receipt = {
                "ok": True,
                "persisted_candidates": int(len(_CANDIDATES)),
                "path": str(_candidate_store_path(_candidate_state_root())),
                "skipped_no_matching_candidate": True,
            }

        rolled_back_nondurable_memory = False
        if not bool(persist_receipt.get("ok")) and rollback_candidates:
            # Restore the exact pre-observation candidate memory so the same
            # immutable source fact can replay the transition after storage
            # recovers.  Any independently committed terminal veto is re-applied.
            for cid, snapshot_before in rollback_candidates.items():
                restored = copy.deepcopy(snapshot_before)
                veto = _terminal_veto_for_episode_locked(cid)
                if veto and not _is_terminal(restored):
                    _apply_cross_layer_terminal(
                        restored, str(veto.get("terminal_state") or "LIVE_FAILED"),
                        row=veto, at=str(veto.get("terminal_at") or now.isoformat()),
                    )
                    restored["rollback_terminal_veto_dominated_candidate_state"] = True
                _CANDIDATES[cid] = restored
            for sym in touched_symbols:
                key = (market, sym)
                ids = {
                    cid for cid, candidate in _CANDIDATES.items()
                    if str(candidate.get("market_key") or "") == market
                    and str(candidate.get("symbol") or "").strip().upper() == sym
                    and not _is_terminal(candidate)
                }
                if ids:
                    _CANDIDATE_IDS_BY_MARKET_SYMBOL[key] = ids
                else:
                    _CANDIDATE_IDS_BY_MARKET_SYMBOL.pop(key, None)
                had_prior_tick, prior_tick = rollback_last_ticks.get(key, (False, None))
                if had_prior_tick:
                    _LAST_TICK_BY_SYMBOL[key] = prior_tick
                else:
                    _LAST_TICK_BY_SYMBOL.pop(key, None)
            live_event_rows = []
            research_event_rows = []
            rolled_back_nondurable_memory = True

    tick_audit_queued = 0
    if bool(persist_receipt.get("ok")):
        for audit_row in tick_audit_intents:
            receipt = _enqueue_research_jsonl(tick_path, audit_row, kind="pulse_tick_audit")
            if receipt.get("queued") or receipt.get("duplicate"):
                tick_audit_queued += 1

    # Never publish/ACK a lifecycle transition before its candidate truth has
    # persisted.  On a failed persist the memory rollback above keeps the exact
    # source observation replayable, so the next successful attempt recreates
    # and publishes the transition exactly once.
    episode_event_results: List[Dict[str, Any]] = []
    if bool(persist_receipt.get("ok")):
        for event_type, event_payload, event_extra, suppress_duplicate in episode_event_intents:
            try:
                event_receipt = dict(_append_episode_event(
                    event_type, event_payload, extra=event_extra,
                    suppress_duplicate=bool(suppress_duplicate),
                ) or {})
            except Exception as exc:
                _record_stage_error(
                    "event_write", "persist_post_candidate_episode_event", exc,
                    market=market, symbol=str(event_payload.get("symbol") or ""),
                    episode_id=str(event_payload.get("episode_id") or event_payload.get("pulse_episode_id") or ""),
                    reason_code="POST_CANDIDATE_EPISODE_EVENT_WRITE_FAILED",
                )
                event_receipt = {"written": False, "reason": f"{type(exc).__name__}:{exc}"}
            episode_event_results.append({"event_type": event_type, **event_receipt})

    # H12H13: preserve H12H12 candidate-scoped causal ordering without paying
    # lifecycle/UI/fsync latency on the serialized Source Truth worker.  The debt
    # was persisted with candidate truth above; enqueue is only an acceleration.
    _immediate_terminal_outbox = None
    if bool(persist_receipt.get("ok")) and immediate_terminal_candidate_ids:
        _immediate_terminal_outbox = {
            "candidates": [], "failed": 0, "async": True,
            "fast_state_owner_attempted": 0, "fast_state_owner_applied": 0,
        }
        for _terminal_cid in immediate_terminal_candidate_ids:
            _state_result = {"attempted": False, "applied": False}
            with _LOCK:
                _terminal_candidate = _CANDIDATES.get(_terminal_cid)
                _terminal_record = None
                if _terminal_candidate is not None:
                    for _item in sorted(
                        [dict(x or {}) for x in list(_terminal_candidate.get("_lifecycle_transition_outbox") or []) if isinstance(x, dict)],
                        key=lambda x: (int(x.get("transition_seq") or 0), str(x.get("transition_id") or "")),
                    ):
                        if _lifecycle_transition_kind(_item) == "TERMINAL":
                            _terminal_record = _item
                            break
            if _terminal_record is not None:
                _state_result = _notify_terminal_state_owner(_terminal_record)
            if bool((_state_result or {}).get("attempted")):
                _immediate_terminal_outbox["fast_state_owner_attempted"] += 1
            if bool((_state_result or {}).get("applied")):
                _immediate_terminal_outbox["fast_state_owner_applied"] += 1
            _queued = _enqueue_terminal_candidate_drain(_terminal_cid)
            _immediate_terminal_outbox["candidates"].append({
                **dict(_queued or {}), "fast_state_owner": dict(_state_result or {}),
            })
            if not bool((_queued or {}).get("queued")):
                _immediate_terminal_outbox["failed"] += 1

    # Projection delivery is intentionally behind the live path.  Candidate +
    # lifecycle/projection debt are already durable at this point, so JSONL or
    # lifecycle-owner latency must not delay R1 UI or Source ACK.  The existing
    # LiveSealTimeOwner drains this outbox in the background/restart path.
    if bool(persist_receipt.get("ok")):
        with _LOCK:
            pending_outbox = sum(
                len(list(candidate.get("_lifecycle_transition_outbox") or []))
                for candidate in _CANDIDATES.values()
            )
        outbox = {
            "attempted": 0, "delivered": 0, "failed": 0,
            "terminal_candidates_removed": 0,
            "terminal_projection_delivered": 0, "terminal_projection_failed": 0,
            "r1_projection_delivered": 0, "r1_projection_failed": 0,
            "projection_results": [],
            "deferred_to_time_owner": True,
            "pending_transitions": int(pending_outbox),
        }
    else:
        outbox = {
            "attempted": 0, "delivered": 0, "failed": 0,
            "terminal_candidates_removed": 0,
            "terminal_projection_delivered": 0, "terminal_projection_failed": 0,
            "skipped_nondurable_candidate_state": True,
        }
    return {
        "enabled": True, "written": written, "active": snapshot().get("active_candidates", 0),
        "matched_candidates": matched_candidates,
        "observation_accepted_ids": sorted({cid for cid in observation_accepted_ids if cid}),
        "observation_accepted": bool(matched_candidates),
        "persisted": bool(persist_receipt.get("ok")),
        "candidate_persist_skipped": bool(persist_receipt.get("skipped_no_matching_candidate")),
        "candidate_store_path": persist_receipt.get("path"),
        "rolled_back_nondurable_memory": bool(rolled_back_nondurable_memory),
        "episode_event_results": episode_event_results,
        "episode_events_deferred_until_persist": len(episode_event_intents),
        "r1_projection_debt_errors": list(projection_debt_errors),
        "lifecycle_outbox": outbox,
        "immediate_terminal_outbox": _immediate_terminal_outbox,
        "terminal_projection_scheduler": terminal_projection_scheduler_snapshot(),
        "live_event_rows": live_event_rows, "live_r1_events": len(live_event_rows),
        "research_event_rows": research_event_rows,
        "research_events": len(research_event_rows),
        "tick_audit_authoritative": False,
        "tick_audit_queued": int(tick_audit_queued),
        "version": VERSION,
    }


def episode_truth(candidate_id: str) -> Dict[str, Any]:
    """Return a defensive copy of one episode's tick truth."""
    with _LOCK:
        _ensure_candidates_restored_locked()
        for alias in _episode_id_aliases(str(candidate_id or "")):
            row = _CANDIDATES.get(alias)
            if row:
                return dict(row)
        return {}


def snapshot() -> Dict[str, Any]:
    with _LOCK:
        return _snapshot_locked()


__all__ = ["VERSION", "register_candidates", "record_price_batch", "episode_truth", "snapshot", "active_markets", "active_candidate_rows", "has_active_candidate_for_symbol", "project_terminal_rows", "ensure_session_close_tombstone", "finalize_session_episodes", "finalize_market_session", "drain_lifecycle_outbox", "drain_research_telemetry", "set_terminal_callback", "set_lifecycle_callback", "terminal_state_for_row", "note_terminal_episode", "terminal_veto_for_episode", "terminal_projection_scheduler_snapshot", "quiesce_terminal_projection_scheduler", "TerminalVetoStoreCorrupt"]
