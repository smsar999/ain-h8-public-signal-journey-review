# -*- coding: utf-8 -*-
"""Strategic ordering for changed source files, separate from model priority."""
from __future__ import annotations

import os
import time
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence, Tuple

from live_sniper_source_quarantine import source_file_resolution_registry
from live_sniper_source_unmapped import source_unmapped_registry
from live_sniper_source_signature import normalize_source_signature, source_signature_from_stat
from source_generation_identity import same_physical_generation

VERSION = "A96_SOURCE_PRIORITY_UNIVERSE_FILTER_V1"
BUCKET_R1 = 0
BUCKET_ACTIVE_CROSS = 1
BUCKET_NEAR = 2
BUCKET_GENERAL = 3


def source_attention_bucket(
    row: Mapping[str, Any], *, lifecycle_truth: Mapping[str, Any] | None = None,
    pulse_state: Mapping[str, Any] | None = None,
) -> int:
    truth = dict(lifecycle_truth or {})
    pulse = dict(pulse_state or {})
    state = str(
        truth.get("live_pulse_seal_state") or truth.get("gann20_episode_state")
        or truth.get("action_state") or ""
    ).upper()
    if bool(truth.get("r1_watch_armed")) or "WAITING_R1" in state or "R1_WATCH" in state:
        return BUCKET_R1
    if state.startswith("LIVE_") or state == "SEAL_AWAITING_FINAL_SCORE" or bool(pulse.get("pulse_new_this_bar")):
        return BUCKET_ACTIVE_CROSS
    if str(pulse.get("pulse_state") or "").lower() == "near":
        return BUCKET_NEAR
    return BUCKET_GENERAL


def _sort_key(item: Tuple[int, Any, float, str, Mapping[str, Any]]) -> tuple[Any, ...]:
    bucket, signature, detected_at, norm, _meta = item
    # Strategic bucket first, then the newest physical source version.  Size is
    # only a deterministic tie-breaker after source and detection time.
    return (bucket, -signature.mtime_ns, -detected_at, -signature.size, norm)


def _decorate_source_items(
    items: Sequence[Tuple[str, Mapping[str, Any]]],
    classify: Callable[[Mapping[str, Any]], int],
) -> List[Tuple[int, Any, float, str, Mapping[str, Any]]]:
    decorated = []
    for norm, meta in items:
        source = dict(meta or {})
        decorated.append((
            int(classify(source)), normalize_source_signature(source.get("signature")),
            float(source.get("detected_at") or 0.0), str(norm), meta,
        ))
    return sorted(decorated, key=_sort_key)


def _general_reserve(cap: int, general_count: int, ratio: float) -> int:
    bounded = max(0.0, min(0.75, float(ratio or 0.0)))
    minimum = 1 if general_count and cap > 1 else 0
    return min(general_count, max(minimum, int(round(cap * bounded))))


def _retry_due(meta: Mapping[str, Any], now_monotonic: float) -> bool:
    try:
        return float((meta or {}).get("source_retry_not_before_monotonic") or 0.0) <= now_monotonic
    except (TypeError, ValueError, OverflowError):
        return True


def next_source_retry_delay(
    bucket: Mapping[str, Mapping[str, Any]], *, now_monotonic: float | None = None,
) -> float | None:
    """Return 0 when work is due, a positive wait, or None for unclassified work."""
    current = time.monotonic() if now_monotonic is None else float(now_monotonic)
    waits: List[float] = []
    for meta in (bucket or {}).values():
        try:
            due = float((meta or {}).get("source_retry_not_before_monotonic") or 0.0)
        except (TypeError, ValueError, OverflowError):
            return None
        if due <= current:
            return 0.0
        waits.append(due - current)
    return min(waits) if waits else None


def order_source_items(
    items: Sequence[Tuple[str, Mapping[str, Any]]], *,
    classify: Callable[[Mapping[str, Any]], int], limit: int,
    general_reserve_ratio: float = 0.25, now_monotonic: float | None = None,
) -> List[Tuple[str, Mapping[str, Any]]]:
    """Prioritize due hot work while guaranteeing a share to general files."""
    cap = max(1, int(limit or 1))
    current = time.monotonic() if now_monotonic is None else float(now_monotonic)
    eligible = [(norm, meta) for norm, meta in items if _retry_due(meta, current)]
    decorated = _decorate_source_items(eligible, classify)
    general = [item for item in decorated if item[0] == BUCKET_GENERAL]
    hot = [item for item in decorated if item[0] != BUCKET_GENERAL]
    reserve = _general_reserve(cap, len(general), general_reserve_ratio)
    selected = hot[:max(0, cap - reserve)]
    selected.extend(general[:reserve])
    remaining_count = cap - len(selected)
    if remaining_count > 0:
        selected_ids = {item[3] for item in selected}
        selected.extend([item for item in decorated if item[3] not in selected_ids][:remaining_count])
    selected.sort(key=_sort_key)
    return [(item[3], item[4]) for item in selected[:cap]]


def source_metadata_by_norm(owner: Any, market: str, paths: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    try:
        rows = owner._live_sniper_rows_for_paths(market, list(paths or []))
    except Exception:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows or []:
        try:
            norm = owner._live_sniper_norm_path((row or {}).get("filename"))
        except Exception:
            continue
        if norm:
            out[str(norm)] = dict(row or {})
    return out


def _pending_source_item(
    owner: Any, market: str, text: str, stat: Any, metadata: Mapping[str, Mapping[str, Any]],
    *, detected_at: float, epoch: int, universe_generation: str = "",
) -> tuple[str, Dict[str, Any]] | None:
    signature = source_signature_from_stat(stat)
    norm = owner._live_sniper_norm_path(text)
    # H12H14H5: the scanner commits (mtime,size,content-fingerprint). Carry the
    # fingerprint into pending/lease identity so a same-stat TickerChart rewrite
    # is a distinct physical generation rather than a duplicate notification.
    scan_fp = ""
    try:
        scan_state = (getattr(owner, "_source_file_stats_by_market", {}) or {}).get(str(market), {}) or {}
        scan_sig = scan_state.get(text) or scan_state.get(str(text))
        if isinstance(scan_sig, (tuple, list)) and len(scan_sig) >= 3:
            scan_fp = str(scan_sig[2] or "")
    except Exception:
        scan_fp = ""
    if not source_unmapped_registry(owner).admit(market, norm, universe_generation):
        return None
    admission = source_file_resolution_registry(owner).admit_signature(market, norm, signature, scan_fp)
    if not admission.allowed:
        return None
    source = dict((metadata or {}).get(norm) or {})
    item = {
        "path": text, "signature": signature, "detected_at": detected_at, "epoch": int(epoch),
        "source_audit_fingerprint": scan_fp,
        "source_generation_id": scan_fp,
        "source_universe_generation": str(universe_generation or ""),
        "symbol": str(source.get("symbol") or "").strip().upper(),
        "name": str(source.get("name") or ""), "fields": int(source.get("fields", 8) or 8),
    }
    if admission.retry_not_before_monotonic > time.monotonic():
        item["source_retry_not_before_monotonic"] = float(admission.retry_not_before_monotonic)
        item["source_resolution_attempts"] = int(admission.attempts)
    return str(norm), item


def _admit_pending_source_request(
    owner: Any, market_key: str, text: str, pending: Dict[str, Dict[str, Any]],
    metadata: Mapping[str, Mapping[str, Any]], *, detected_at: float, epoch: int,
    universe_generation: str,
) -> str:
    if not text.upper().endswith(".DAT") or not os.path.isfile(text):
        return "rejected"
    try:
        stat_result = os.stat(text)
        # MetaStock MASTER may expose group/index placeholders as F*.DAT entries.
        # A zero-byte generation contains no price record and must never consume a
        # SOURCE_PRIORITY lease/read/retry budget.  A later non-zero signature is
        # admitted normally on the next physical change notification.
        if int(getattr(stat_result, "st_size", 0) or 0) <= 0:
            return "parked"
        resolved = _pending_source_item(
            owner, market_key, text, stat_result, metadata,
            detected_at=detected_at, epoch=epoch, universe_generation=universe_generation,
        )
    except OSError:
        return "rejected"
    if resolved is None:
        return "rejected"
    norm, item = resolved
    old = pending.get(norm)
    if old is not None and same_physical_generation(old, item):
        return "superseded"
    pending[norm] = item
    return "accepted"


def update_pending_source_files(
    owner: Any, changed_files: Iterable[str], pending: Dict[str, Dict[str, Any]],
    metadata: Mapping[str, Mapping[str, Any]], *, detected_at: float, epoch: int,
    market: str | None = None, universe_generation: str = "",
) -> Dict[str, int]:
    market_key = str(market or getattr(owner, "_active_market_key", "") or "")
    requested_paths = list(dict.fromkeys(str(path or "") for path in (changed_files or [])))
    stats = {"requested": len(requested_paths), "accepted": 0, "superseded": 0, "rejected": 0, "parked": 0}
    for text in requested_paths:
        outcome = _admit_pending_source_request(
            owner, market_key, text, pending, metadata, detected_at=detected_at,
            epoch=epoch, universe_generation=str(universe_generation or ""),
        )
        stats[outcome] += 1
    return stats


def rows_for_selected(
    owner: Any, market: str, selected: Sequence[Tuple[str, Mapping[str, Any]]],
) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Compatibility wrapper over the per-file resolution contract."""
    from live_sniper_source_resolution import resolve_selected_source_rows

    resolved = resolve_selected_source_rows(owner, market, selected)
    return [dict(row) for row in resolved.rows], dict(resolved.meta_by_norm)


def source_attention_bucket_for_owner(owner: Any, market: str, meta: Mapping[str, Any], pulse_map: Mapping[str, Mapping[str, Any]] | None = None) -> int:
    """Classify one source generation against the owner's current episode truth."""
    symbol = str((meta or {}).get("symbol") or "").strip().upper()
    truth: Dict[str, Any] = {}
    try:
        engine = getattr(owner, "_live_pulse_seal_engine", None)
        truth = dict(engine.truth_for_symbol(market, symbol) or {}) if engine is not None and symbol else {}
    except Exception:
        truth = {}
    pulse = dict((pulse_map or {}).get(symbol) or {}) if pulse_map is not None else {}
    if pulse_map is None:
        try:
            tape = owner._pulse_tape_for_market(market)
            pulse = dict((tape.pulse_map() or {}).get(symbol) or {}) if tape is not None and symbol else {}
        except Exception:
            pulse = {}
    return source_attention_bucket(meta, lifecycle_truth=truth, pulse_state=pulse)


def order_owner_source_bucket(owner: Any, market: str, bucket: Mapping[str, Mapping[str, Any]], limit: int) -> List[Tuple[str, Mapping[str, Any]]]:
    """Resolve current lifecycle/pulse state once and order one pending source wave."""
    pulse_map: Dict[str, Dict[str, Any]] = {}
    try:
        tape = owner._pulse_tape_for_market(market)
        pulse_map = dict(tape.pulse_map() or {}) if tape is not None else {}
    except Exception:
        pulse_map = {}

    def classify(meta: Mapping[str, Any]) -> int:
        return source_attention_bucket_for_owner(owner, market, meta, pulse_map)

    return order_source_items(list((bucket or {}).items()), classify=classify, limit=limit, now_monotonic=time.monotonic())


__all__ = [
    "VERSION", "BUCKET_R1", "BUCKET_ACTIVE_CROSS", "BUCKET_NEAR", "BUCKET_GENERAL",
    "source_attention_bucket", "source_attention_bucket_for_owner", "order_source_items", "order_owner_source_bucket", "source_metadata_by_norm",
    "next_source_retry_delay", "update_pending_source_files", "rows_for_selected",
]
