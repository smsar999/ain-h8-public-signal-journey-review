# -*- coding: utf-8 -*-
"""Append-only GANN20 episode truth ledger for R163.

The ledger is diagnostic and non-blocking.  It never decides a trade and never
rewrites prior events.  Every stage uses the same episode id from the first
observed cross bar, allowing Session Feedback, pulse tick tape, Portfolio Center
and UI to reconstruct the exact path without hindsight.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from gann20_episode_contract import VERSION as CONTRACT_VERSION
from gann20_episode_truth_writer import prepare as _prepare_truth_event, payload_fields as _truth_payload_fields
from gann20_event_truth_projection import (
    payload_fields as _a99_event_truth_fields,
    signature_fields as _a99_event_signature_fields,
)
from atomic_io_utils import sync_file, sync_parent_directory
from live_sniper_contract import (
    VERSION as LIVE_SNIPER_CONTRACT_VERSION,
    LIVE_P30_BORN, LIVE_P30_UPDATED, SEALED_P30_CONFIRMED,
    SEALED_P30_LATE_CENTER_ONLY, P30_DOWNGRADED_TO_R1_WATCH,
    SEALED_R1_WATCH_ARMED, LIVE_R1_BORN, LIVE_R1_UPDATED,
    TRADER_EVENT_CANCELLED, ORIGIN_LIVE_SOURCE_OBSERVATION,
    ORIGIN_LIVE_PRICE_TICK, P30_HIDE_EVENTS,
    p30_birth_is_valid, r1_birth_is_valid, date_text as _sniper_date_text,
)

VERSION = "A4_2_8_SEMANTIC_ONE_SHOT_LEDGER_V1"
_LOCK = threading.RLock()
_LAST_SIGNATURE: Dict[str, str] = {}
_READ_CACHE: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
_PROJECTION_SEEN: set[str] = set()
# Events whose business meaning is one transition per Episode.  Volatile fields
# (P50, UI flags, counters, etc.) must never turn the same transition into a new
# ledger event.  The persisted ledger is consulted lazily so idempotency survives
# process restart, not just the current Python process.
_SEMANTIC_ONE_SHOT_EVENTS = frozenset({
    "LIVE_R1_WATCH_ARMED",
    "PRE_ENTRY_STOP_INVALIDATED",
})
_ONE_SHOT_SEEN: set[str] = set()


def _root() -> Path:
    configured = str(os.environ.get("AIN_GANN20_EPISODE_LEDGER_DIR", "") or "").strip()
    return Path(configured) if configured else Path(__file__).resolve().parent / "datainfo" / "gann20_episode_ledger"


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _slug(value: Any) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9_\-]+", "_", str(value or "market")).strip("_")[:96] or "market"


def _first_present(source: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = (source or {}).get(key)
        if value is not None and not (isinstance(value, str) and value.strip() in {"", "-", "nan", "None", "null"}):
            return value
    return None



def _semantic_one_shot_key(episode_id: str, event_type: str) -> str:
    # Scope the in-memory cache to the active ledger root.  Acceptance sessions
    # use isolated runtime roots; one process must never suppress an event merely
    # because another isolated root used the same Episode ID in a prior test/run.
    try:
        root_key = str(_root().resolve())
    except Exception:
        root_key = str(_root())
    return f"{root_key}|{str(episode_id or '').strip()}|{str(event_type or '').strip().upper()}"


def _semantic_one_shot_paths(market: str, day: str) -> List[Path]:
    """Candidate persisted ledgers for one exchange-session event.

    Search the selected day plus adjacent calendar days because FX and delayed
    audit writes may cross a computer-day boundary while retaining one market
    session identity.  Include alias-named market files too: authority keys may
    use underscore-safe aliases while the Episode ID remains canonical.
    """
    try:
        base = _dt.date.fromisoformat(str(day or '')[:10])
    except Exception:
        return []
    exact_name = f"{_slug(market)}_gann20_episode_events.jsonl"
    out: List[Path] = []
    for delta in (-1, 0, 1):
        folder = _root() / (base + _dt.timedelta(days=delta)).isoformat()
        exact = folder / exact_name
        if exact.is_file():
            out.append(exact)
        if folder.is_dir():
            out.extend(sorted(folder.glob("*_gann20_episode_events.jsonl")))
    return list(dict.fromkeys(out))


def _semantic_one_shot_persisted(episode_id: str, event_type: str, *, market: str, day: str) -> bool:
    key = _semantic_one_shot_key(episode_id, event_type)
    if key in _ONE_SHOT_SEEN:
        return True
    for path in _semantic_one_shot_paths(market, day):
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    persisted_episode = str(row.get("pulse_episode_id") or row.get("episode_id") or "").strip()
                    persisted_event = str(row.get("event_type") or "").strip().upper()
                    if persisted_episode == str(episode_id or "").strip() and persisted_event == str(event_type or "").strip().upper():
                        _ONE_SHOT_SEEN.add(key)
                        return True
        except OSError:
            # Ledger I/O failure is handled by the normal append path; duplicate
            # probing must not turn observability into a new trading failure.
            continue
    return False


def _projection_cache_key(projection_id: str) -> str:
    try:
        root_key = str(_root().resolve())
    except Exception:
        root_key = str(_root())
    return f"{root_key}|{str(projection_id or '').strip()}"


def _projection_persisted_path(projection_id: str, *, market: str, day: str) -> Optional[Path]:
    """Return the ledger path containing one deterministic projection.

    Presence is sufficient for semantic idempotency, but Hotfix9 deliberately
    keeps durability separate: a prior append may have reached the page cache
    and then failed fsync.  The caller therefore forces the existing file before
    treating it as a durable success and clearing projection debt.
    """
    wanted = str(projection_id or "").strip()
    if not wanted:
        return None
    for path in _semantic_one_shot_paths(market, day):
        if not path.is_file():
            continue
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if str(row.get("projection_id") or "").strip() == wanted:
                        return path
        except OSError:
            continue
    return None


def _append_payload_line(path: Path, payload: Mapping[str, Any], *, durable: bool = False) -> None:
    """Append one JSONL projection row.

    The lifecycle/candidate snapshot remains the authority.  Most research
    events are intentionally best-effort and do not fsync.  ``LIVE_R1_BORN``
    projection obligations are rare decision-bearing projections: the outbox
    may clear their debt only after this append has reached the OS durability
    primitive.  Because projection delivery runs behind the live path, this
    fsync never gates source->UI latency.
    """
    existed_before = path.exists()
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_separator = False
    if path.is_file() and path.stat().st_size > 0:
        try:
            with path.open("rb") as reader:
                reader.seek(-1, os.SEEK_END)
                needs_separator = reader.read(1) != b"\n"
        except OSError:
            needs_separator = False
    encoded = (json.dumps(payload, ensure_ascii=False, default=str, separators=(",", ":")) + "\n").encode("utf-8")
    with path.open("ab") as handle:
        if needs_separator:
            handle.write(b"\n")
        handle.write(encoded)
        handle.flush()
        if durable:
            os.fsync(handle.fileno())
    if durable and not existed_before:
        # POSIX needs the new directory entry forced as well.  The helper is a
        # no-op on Windows, where Python exposes no O_DIRECTORY contract.
        sync_parent_directory(path)



def append_event(event_type: str, row: Mapping[str, Any], *, extra: Optional[Mapping[str, Any]] = None,
                 suppress_duplicate: bool = True) -> Dict[str, Any]:
    prepared = _prepare_truth_event(event_type, row or {}, extra or {}, producer_version=VERSION); source, event_extra = prepared["source"], prepared["extra"]; episode_id, market, symbol, event_origin = prepared["episode_id"], prepared["market"], prepared["symbol"], prepared["event_origin"]
    if prepared["error"]: return {"written": False, "reason": "missing_truth_contract", "detail": prepared["error"]}
    if not episode_id: return {"written": False, "reason": "missing_episode_id"}
    payload = {
        "ledger_version": VERSION,
        "contract_version": CONTRACT_VERSION,
        "recorded_at": _now(),
        "event_type": str(event_type or "UNKNOWN"),
        "pulse_episode_id": episode_id,
        **_truth_payload_fields(source, event_origin, episode_id),
        "market_key": market,
        "symbol": symbol,
        "market_session_date": source.get("live_pulse_session_date") or source.get("session_signal_date") or source.get("market_session_date") or source.get("session_data_date"),
        "event_observed_at": source.get("first_r1_crossed_at") or source.get("gann20_activation_observed_at") or source.get("gann20_r1_first_hit_time") or source.get("first_r1_tick_ts") or source.get("appearance_at") or source.get("signal_detected_at"),
        "first_cross_bar": source.get("first_cross_bar") or source.get("signal_bar_time") or source.get("pulse_bar_time"),
        "first_cross_at": source.get("first_cross_at") or source.get("signal_detected_at"),
        "first_cross_price": source.get("first_cross_price"),
        "probability_scope": source.get("probability_scope") or source.get("gann20_probability_scope"),
        "probability_asof": source.get("probability_asof"),
        "probability_bar_time": source.get("probability_bar_time"),
        "probability_scope_first_cross": source.get("probability_scope_first_cross"),
        "probability_source_observation_id": source.get("probability_source_observation_id"),
        "probability_at_cross_exact": source.get("probability_at_cross_exact") or source.get("probability_exact_at_cross"),
        "p50_at_first_cross": _first_present(source, "p50_at_first_cross", "p50_at_cross"),
        "p100_at_first_cross": source.get("p100_at_first_cross"),
        "p50_at_first_threshold": _first_present(source, "p50_at_first_threshold", "p50_at_qualification"),
        "p100_at_first_threshold": _first_present(source, "p100_at_first_threshold", "p100_at_qualification"),
        "p50_live": _first_present(source, "p50_live", "gann20_p_r50_pct"),
        "p100_live": _first_present(source, "p100_live", "gann20_p_r100_pct"),
        "p50_sealed": _first_present(source, "p50_sealed", "p50_final"),
        "p100_sealed": _first_present(source, "p100_sealed", "p100_final"),
        **_a99_event_truth_fields(source, _first_present),
        "sealed_model_anchor_at": _first_present(source, "sealed_model_anchor_at", "sealed_at"),
        "sealed_model_anchor_price": source.get("sealed_model_anchor_price"),
        "model_horizon_bars": source.get("model_horizon_bars"),
        "model_bars_elapsed": source.get("model_bars_elapsed"),
        "model_bars_remaining": source.get("model_bars_remaining"),
        "r1_frozen": _first_present(source, "r1_frozen", "gann_r1_breakout_point"),
        "r50_frozen": _first_present(source, "r50_frozen", "gann_r3_resistance_50"),
        "r100_frozen": _first_present(source, "r100_frozen", "gann_r5_resistance_100"),
        "episode_state": source.get("gann20_episode_state") or source.get("live_pulse_seal_state") or source.get("action_state"),
        "execution_passed": _first_present(source, "execution_passed", "execution_passed_now"),
        "execution_decision": source.get("execution_decision") or source.get("execution_decision_now"),
        "published_to_trader": _first_present(source, "published_to_trader", "live_publishable"),
        "source_observation_id": source.get("source_observation_id") or source.get("probability_source_observation_id"),
        "truth_scope": source.get("truth_scope") or source.get("probability_scope"),
        "snapshot_review_only": source.get("snapshot_review_only"),
        "saved_snapshot_review": source.get("saved_snapshot_review"),
        "historical_reconstructed": source.get("historical_reconstructed"),
        "historical_lastbar_review": source.get("historical_lastbar_review"),
        "data_stale": source.get("data_stale"),
        "live_sniper_contract_version": source.get("live_sniper_contract_version") or LIVE_SNIPER_CONTRACT_VERSION,
        "live_sniper_event_type": source.get("live_sniper_event_type") or event_type,
        "live_sniper_event_origin": source.get("live_sniper_event_origin") or event_origin,
        "live_sniper_birth_proven": source.get("live_sniper_birth_proven"),
        "live_sniper_p30_birth_proven": source.get("live_sniper_p30_birth_proven"),
        "live_sniper_r1_birth_proven": source.get("live_sniper_r1_birth_proven"),
        "live_sniper_birth_kind": source.get("live_sniper_birth_kind"),
        "live_sniper_born_at": source.get("live_sniper_born_at"),
        "live_sniper_source_observation_id": source.get("live_sniper_source_observation_id"),
        "trader_board_session_date": source.get("trader_board_session_date") or source.get("live_sniper_session_date"),
        "signal_entry_price": source.get("signal_entry_price") or source.get("entry_signal_price"),
        "first_r1_crossed_at": source.get("first_r1_crossed_at") or source.get("first_r1_tick_ts"),
        "first_r1_price": source.get("first_r1_price") or source.get("r1_activation_price"),
        "r1_watch_armed": source.get("r1_watch_armed"),
        "r1_future_bar_cross": source.get("r1_future_bar_cross"),
        "r1_activation_reconstructed": source.get("r1_activation_reconstructed"),
        **event_extra,
    }
    signature = json.dumps({
        "event_type": payload["event_type"],
        "episode_state": payload.get("episode_state"),
        "probability_scope": payload.get("probability_scope"),
        "p50_live": payload.get("p50_live"),
        "p50_sealed": payload.get("p50_sealed"),
        **_a99_event_signature_fields(payload),
        "model_bars_elapsed": payload.get("model_bars_elapsed"),
        "execution_passed": payload.get("execution_passed"),
        "published_to_trader": payload.get("published_to_trader"),
    }, ensure_ascii=False, sort_keys=True, default=str)
    key = f"{episode_id}|{payload['event_type']}"
    # Store by exchange session date, not by the computer day on which a
    # delayed/replayed event happened to be written.  Otherwise a same-session
    # board restore can miss valid events around midnight or during later audit.
    day = _date_text(payload.get("market_session_date") or payload.get("trader_board_session_date") or payload.get("event_observed_at") or payload.get("recorded_at")) or str(_now())[:10]
    path = _root() / day / f"{_slug(market)}_gann20_episode_events.jsonl"
    semantic_one_shot = str(payload.get("event_type") or "").strip().upper() in _SEMANTIC_ONE_SHOT_EVENTS
    projection_id = str(payload.get("projection_id") or "").strip()
    with _LOCK:
        existing_projection_path = (
            _projection_persisted_path(projection_id, market=market, day=day)
            if suppress_duplicate and projection_id else None
        )
        if existing_projection_path is not None:
            try:
                if str(payload.get("event_type") or "").strip().upper() == LIVE_R1_BORN:
                    sync_file(existing_projection_path)
                _PROJECTION_SEEN.add(_projection_cache_key(projection_id))
                return {
                    "written": False,
                    "reason": "idempotent_existing_projection",
                    "idempotent_success": True,
                    "durable": True,
                    "projection_id": projection_id,
                    "episode_id": episode_id,
                    "path": str(existing_projection_path),
                }
            except Exception as exc:
                return {
                    "written": False,
                    "reason": f"EXISTING_PROJECTION_DURABILITY_FAILED:{type(exc).__name__}:{exc}",
                    "idempotent_success": False,
                    "durable": False,
                    "projection_id": projection_id,
                    "episode_id": episode_id,
                    "path": str(existing_projection_path),
                }
        if suppress_duplicate and semantic_one_shot and _semantic_one_shot_persisted(
            episode_id, payload["event_type"], market=market, day=day,
        ):
            return {"written": False, "reason": "duplicate_semantic_one_shot", "episode_id": episode_id}
        if suppress_duplicate and not projection_id and not semantic_one_shot and _LAST_SIGNATURE.get(key) == signature:
            return {"written": False, "reason": "duplicate", "episode_id": episode_id}
        try:
            durable_projection = bool(
                projection_id
                and str(payload.get("event_type") or "").strip().upper() == LIVE_R1_BORN
            )
            _append_payload_line(path, payload, durable=durable_projection)
            # In-memory dedup is committed only *after* the append succeeds.
            _LAST_SIGNATURE[key] = signature
            if projection_id:
                _PROJECTION_SEEN.add(_projection_cache_key(projection_id))
            if semantic_one_shot:
                _ONE_SHOT_SEEN.add(_semantic_one_shot_key(episode_id, payload["event_type"]))
            _READ_CACHE.clear()
            return {
                "written": True,
                "durable": bool(durable_projection),
                "episode_id": episode_id,
                "path": str(path),
                "projection_id": projection_id or None,
                "idempotent_success": False,
            }
        except Exception as exc:
            return {"written": False, "reason": f"{type(exc).__name__}: {exc}", "episode_id": episode_id}



def _date_text(value: Any) -> str:
    text = str(value or "").strip().replace("T", " ")
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]
    return ""


def _candidate_ledger_files(market_key: str, session_date: str) -> List[Path]:
    try:
        day = _dt.date.fromisoformat(str(session_date)[:10])
    except Exception:
        return []
    files: List[Path] = []
    for delta in (-1, 0, 1):
        folder = _root() / (day + _dt.timedelta(days=delta)).isoformat()
        if not folder.exists():
            continue
        exact = folder / f"{_slug(market_key)}_gann20_episode_events.jsonl"
        if exact.exists():
            files.append(exact)
        else:
            files.extend(sorted(folder.glob("*_gann20_episode_events.jsonl")))
    # Preserve order while deduplicating.
    return list(dict.fromkeys(files))


def read_session_pulse_births(market_key: str, session_date: str) -> Dict[str, Dict[str, Any]]:
    """Replay strict R166 trader-board births for one exchange session.

    This is a state replay, not a search for old labels.  Only an exact live P30
    birth or a future-bar live R1 birth may create board identity.  A sealed P30
    downgrade/rejection cancels P30 identity immediately, while historical and
    reconstructed events can never create or revive trader visibility.
    """
    market = str(market_key or "").strip()
    session = str(session_date or "")[:10]
    if not market or not session:
        return {}
    paths = _candidate_ledger_files(market, session)
    signature = tuple((str(path), int(path.stat().st_mtime_ns), int(path.stat().st_size)) for path in paths if path.exists())
    cache_key = (str(_root().resolve()), market, session)
    cached = _READ_CACHE.get(cache_key)
    if cached and cached.get("signature") == signature:
        return {str(k): dict(v or {}) for k, v in dict(cached.get("births") or {}).items()}

    state_by_episode: Dict[str, Dict[str, Any]] = {}
    key_aliases: Dict[str, set[str]] = {}
    ordered_events: List[Dict[str, Any]] = []
    for path in paths:
        try:
            with path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    try:
                        event = json.loads(line)
                    except Exception:
                        continue
                    if str(event.get("market_key") or "").strip() != market:
                        continue
                    ordered_events.append(dict(event or {}))
        except Exception:
            continue
    ordered_events.sort(key=lambda e: (str(e.get("recorded_at") or ""), str(e.get("event_observed_at") or "")))

    for event in ordered_events:
        event_type = str(event.get("live_sniper_event_type") or event.get("event_type") or "").strip().upper()
        episode_id = str(event.get("pulse_episode_id") or "").strip()
        symbol = str(event.get("symbol") or "").strip().upper()
        first_bar = str(event.get("first_cross_bar") or "").strip()
        if not episode_id:
            if not (symbol and first_bar):
                continue
            episode_id = f"{symbol}|{first_bar[:19]}"
        aliases = {episode_id}
        if symbol and first_bar:
            aliases.update({f"{symbol}|{first_bar[:19]}", f"{symbol}|{first_bar[:16]}"})
        key_aliases.setdefault(episode_id, set()).update(aliases)

        row = dict(event or {})
        row["event_type"] = event_type
        row["live_sniper_event_type"] = event_type
        row["market_key"] = market
        row.setdefault("trader_board_session_date", session)
        row.setdefault("market_session_date", event.get("market_session_date") or session)
        if event_type in {LIVE_P30_BORN, LIVE_P30_UPDATED, SEALED_P30_CONFIRMED}:
            # Legacy event names are deliberately ignored unless they carry explicit
            # strict R166 provenance; old P30_LIVE_PUBLISHED polluted prior ledgers.
            if p30_birth_is_valid(row, session):
                born_at = row.get("live_sniper_born_at") or row.get("first_cross_at") or row.get("event_observed_at")
                state_by_episode[episode_id] = {
                    "live_sniper_birth_proven": True,
                    "live_sniper_p30_birth_proven": True,
                    "live_sniper_birth_kind": "P30",
                    "live_sniper_born_at": str(born_at or ""),
                    "live_sniper_session_date": session,
                    "trader_board_session_date": session,
                    "live_sniper_event_type": event_type,
                    "live_sniper_event_origin": ORIGIN_LIVE_SOURCE_OBSERVATION,
                    "live_sniper_source_observation_id": row.get("live_sniper_source_observation_id") or row.get("source_observation_id"),
                    "signal_entry_price": row.get("signal_entry_price") or row.get("first_cross_price"),
                    "first_cross_bar": first_bar or None,
                    "first_cross_at": row.get("first_cross_at"),
                    "first_cross_price": row.get("first_cross_price"),
                    "p50_at_first_cross": row.get("p50_at_first_cross"),
                    "p100_at_first_cross": row.get("p100_at_first_cross"),
                    "probability_scope_first_cross": row.get("probability_scope_first_cross"),
                    "probability_source_observation_id": row.get("probability_source_observation_id"),
                    "probability_at_cross_exact": True,
                    "trader_session_keep_until_new_day": True,
                    "live_sniper_birth_reason_ar": "أُعيدت نبضة P30 المثبتة من تكة التقاطع نفسها.",
                }
        elif event_type in {LIVE_R1_BORN, LIVE_R1_UPDATED}:
            if r1_birth_is_valid(row, session):
                born_at = row.get("live_sniper_born_at") or row.get("first_r1_crossed_at") or row.get("event_observed_at")
                state_by_episode[episode_id] = {
                    "live_sniper_birth_proven": True,
                    "live_sniper_r1_birth_proven": True,
                    "live_sniper_birth_kind": "R1",
                    "live_sniper_born_at": str(born_at or ""),
                    "live_sniper_session_date": session,
                    "trader_board_session_date": session,
                    "live_sniper_event_type": event_type,
                    "live_sniper_event_origin": ORIGIN_LIVE_PRICE_TICK,
                    "signal_entry_price": row.get("signal_entry_price") or row.get("first_r1_price") or row.get("tick_price"),
                    "first_r1_crossed_at": row.get("first_r1_crossed_at") or row.get("first_r1_tick_ts"),
                    "first_r1_price": row.get("first_r1_price") or row.get("tick_price"),
                    "r1_watch_armed": True,
                    "r1_future_bar_cross": True,
                    "p50_sealed": row.get("p50_sealed"),
                    "p100_sealed": row.get("p100_sealed"),
                    "first_cross_bar": first_bar or None,
                    "trader_session_keep_until_new_day": True,
                    "live_sniper_birth_reason_ar": "أُعيد اختراق R1 الحي الأول من دفتر التكات.",
                }
        elif event_type in set(P30_HIDE_EVENTS) | {"SEALED_REJECTED", "LIVE_FAILED", "EXPIRED_SESSION_CLOSE"}:
            current = state_by_episode.get(episode_id)
            if current and str(current.get("live_sniper_birth_kind") or "").upper() == "P30":
                state_by_episode.pop(episode_id, None)

    births: Dict[str, Dict[str, Any]] = {}
    for episode_id, proof in state_by_episode.items():
        for key in key_aliases.get(episode_id, {episode_id}):
            births[key] = dict(proof)
    _READ_CACHE[cache_key] = {"signature": signature, "births": births}
    return {str(k): dict(v or {}) for k, v in births.items()}


def restore_session_pulse_births(rows: List[Mapping[str, Any]], market_key: str, session_date: str) -> List[Dict[str, Any]]:
    births = read_session_pulse_births(market_key, session_date)
    if not births:
        return [dict(row or {}) for row in (rows or [])]
    out: List[Dict[str, Any]] = []
    for source in rows or []:
        row = dict(source or {})
        episode_id = str(row.get("pulse_episode_id") or row.get("episode_id") or row.get("id") or row.get("identifier") or "").strip()
        symbol = str(row.get("symbol") or "").strip().upper()
        bar = str(row.get("first_cross_bar") or row.get("signal_bar_time") or row.get("recommendation_datetime") or "").strip()
        proof = births.get(episode_id)
        if not proof and symbol and bar:
            proof = births.get(f"{symbol}|{bar[:19]}") or births.get(f"{symbol}|{bar[:16]}")
        if proof:
            # Birth identity is immutable; current price/outcome fields remain from
            # the latest row and are not replaced here.
            for key, value in proof.items():
                if value not in (None, ""):
                    row[key] = value
        out.append(row)
    return out


__all__ = ["VERSION", "append_event", "read_session_pulse_births", "restore_session_pulse_births"]
