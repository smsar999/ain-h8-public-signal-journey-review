# -*- coding: utf-8 -*-
"""Executable SOURCE_PRIORITY worker kernel with transactional wave recovery."""
from __future__ import annotations

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
import logging
import os
import sys
import time
import threading
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple

from live_sniper_source_priority import (
    next_source_retry_delay, order_owner_source_bucket, source_attention_bucket_for_owner,
)
from live_source_processing_scheduler import (
    SourceProcessingTask, source_processing_capacity_hints, source_processing_scheduler_for_owner,
)
from live_sniper_source_quarantine import source_file_resolution_registry
from live_sniper_source_resolution import resolve_selected_source_rows
from live_sniper_source_unmapped import source_universe_snapshot, source_unmapped_registry
from live_source_bar_freshness import assess_live_source_bar
from metastock_live_layout_guard import SOURCE_PARTIAL_WRITE, SOURCE_REWRITE_IN_PROGRESS
from source_lease_accounting import source_lease_accounting_for_owner

VERSION = "A4_2_14_SOURCE_CLOCK_TRUTH_HOTFIX12H6_US_PROTECTED_SOURCE_CAPACITY_HOTFIX12H14H7_V1"
_LOG = logging.getLogger(__name__)
SourceSelection = Sequence[Tuple[str, Mapping[str, Any]]]
ErrorReporter = Optional[Callable[..., Any]]


def _merge_source_lane_stats(totals: Dict[str, Any], stats: Mapping[str, Any]) -> None:
    """Merge decision stats without turning textual receipts into retry failures.

    Hotfix10 returned a mixed diagnostic mapping: counters plus receipt/status
    strings such as ``DURABLE_ADVANCE`` and ``SOURCE_JOURNAL_ENQUEUED``.
    Treating every value as ``int`` raised after the observation had already
    been committed, which falsely requeued the physical DAT generation.

    Numeric/bool values remain additive counters.  Non-numeric values are
    diagnostic-only and are copied under ``last_*`` keys; they can never make
    a successful source observation look failed.
    """
    for key, value in dict(stats or {}).items():
        if isinstance(value, bool):
            totals[key] = int(totals.get(key, 0) or 0) + int(value)
            continue
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            try:
                if isinstance(value, float) and not (value == value):
                    continue
                totals[key] = int(totals.get(key, 0) or 0) + int(value)
            except Exception:
                continue
            continue
        # Preserve useful receipt/status observability without polluting the
        # counter namespace.  These fields are intentionally non-authoritative.
        if value not in (None, "", [], {}, ()):
            totals[f"last_{key}"] = value


def empty_source_lane_totals() -> Dict[str, int]:
    return {
        "files": 0, "observations": 0, "raw_crosses": 0,
        "probability_candidates": 0, "probability_scheduled": 0,
        "score_observations": 0, "events": 0, "live_r1_events": 0,
        "duplicates": 0, "errors": 0, "unresolved": 0,
        "requeued": 0, "quarantined": 0, "ignored_unmapped": 0,
        "stale_bars_rejected": 0,
        "lease_recovered": 0,
        "expected_cancel_lease_recovered": 0,
        "unexpected_cancel_lease_recovered": 0,
        "partial_writes": 0, "writer_rewrites": 0, "budget_deferred": 0,
        "order_empty_deferred": 0,
        "processing_enqueued": 0, "processing_queue_full": 0,
        "coalesce_probe_unavailable_preserved": 0,
        "lease_open_duplicate_suppressed": 0,
        "source_resolution_ms_total": 0.0, "source_resolution_ms_max": 0.0,
        "source_resolution_budget_exhausted": 0,
    }


@dataclass(frozen=True)
class SourceLaneRunResult:
    totals: Dict[str, int]
    fatal_error: Optional[Exception] = None
    defer_delay_sec: float = 0.0



# H12H14H3 F52: ThreadPool reads that outlive the bounded SOURCE_PRIORITY wave
# are part of source truth even though the coordinator intentionally stops waiting
# for them.  Freeze shutdown must be able to wait until every such read callback
# has settled before deciding whether the physical pending map is empty.
_DEFERRED_READ_STATE_ATTR = "_live_sniper_source_deferred_read_state_h3"

def _deferred_read_state(owner: Any) -> Dict[str, Any]:
    state = getattr(owner, _DEFERRED_READ_STATE_ATTR, None)
    if isinstance(state, dict) and isinstance(state.get("cv"), threading.Condition):
        return state
    lock = threading.RLock()
    state = {
        "cv": threading.Condition(lock),
        "outstanding": 0,
        "registered": 0,
        "completed": 0,
        "callback_errors": 0,
    }
    setattr(owner, _DEFERRED_READ_STATE_ATTR, state)
    return state

def _deferred_read_registered(owner: Any) -> None:
    state = _deferred_read_state(owner)
    cv = state["cv"]
    with cv:
        state["outstanding"] = int(state.get("outstanding", 0) or 0) + 1
        state["registered"] = int(state.get("registered", 0) or 0) + 1
        cv.notify_all()

def _deferred_read_completed(owner: Any, *, callback_error: bool = False) -> None:
    state = _deferred_read_state(owner)
    cv = state["cv"]
    with cv:
        state["outstanding"] = max(0, int(state.get("outstanding", 0) or 0) - 1)
        state["completed"] = int(state.get("completed", 0) or 0) + 1
        if callback_error:
            state["callback_errors"] = int(state.get("callback_errors", 0) or 0) + 1
        cv.notify_all()

def quiesce_owner_deferred_source_reads(owner: Any, timeout: float = 5.0) -> bool:
    state = _deferred_read_state(owner)
    deadline = time.monotonic() + max(0.1, float(timeout))
    cv = state["cv"]
    with cv:
        while int(state.get("outstanding", 0) or 0) > 0:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            cv.wait(timeout=min(0.10, remaining))
        return True

def source_runtime_shutdown_snapshot(owner: Any) -> Dict[str, Any]:
    state = _deferred_read_state(owner)
    cv = state["cv"]
    with cv:
        deferred = {
            "deferred_reads_outstanding": int(state.get("outstanding", 0) or 0),
            "deferred_reads_registered": int(state.get("registered", 0) or 0),
            "deferred_reads_completed": int(state.get("completed", 0) or 0),
            "deferred_read_callback_errors": int(state.get("callback_errors", 0) or 0),
        }
    pending_by_market: Dict[str, int] = {}
    lock = getattr(owner, "_live_sniper_source_lock", None)
    try:
        if lock is not None:
            with lock:
                pending = dict(getattr(owner, "_live_sniper_source_pending_by_market", {}) or {})
        else:
            pending = dict(getattr(owner, "_live_sniper_source_pending_by_market", {}) or {})
        for market, bucket in pending.items():
            count = len(dict(bucket or {}))
            if count:
                pending_by_market[str(market)] = int(count)
    except Exception:
        deferred["pending_snapshot_error"] = 1
        pending_by_market = {}
    deferred["pending_physical_generations"] = int(sum(pending_by_market.values()))
    deferred["pending_by_market"] = pending_by_market
    return deferred


def _source_lane_run_limits(owner: Any, market: str, worker_count: Callable[[str], int]) -> Tuple[float, int, int]:
    """Return bounded run budget, worker hint and per-run file cap."""
    try:
        budget = max(0.25, min(10.0, float(getattr(owner, "_live_sniper_source_run_budget_sec", os.getenv("AIN_LIVE_SNIPER_SOURCE_RUN_BUDGET_SEC", "2.0")) or 2.0)))
    except Exception:
        budget = 2.0
    try:
        workers = max(1, int(worker_count(market)))
    except Exception:
        workers = 1
    try:
        explicit_cap = getattr(owner, "_live_sniper_source_max_files_per_run", None)
        env_cap = os.getenv("AIN_LIVE_SNIPER_SOURCE_MAX_FILES_PER_RUN")
        if explicit_cap is not None:
            configured_cap = int(explicit_cap)
        elif env_cap not in (None, ""):
            configured_cap = int(env_cap)
        else:
            # A4 Saudi live court: keep one wave to one read-concurrency group.
            # Doubling readers from 4->8 therefore does not double the amount of
            # lifecycle work committed per wave.  Other markets retain the old
            # workers*2 default, and every deployment can still override it.
            configured_cap = workers if "السوق السعودي" in str(market or "") else workers * 2
        cap = max(workers, min(64, configured_cap))
    except Exception:
        cap = workers if "السوق السعودي" in str(market or "") else workers * 2
    return budget, workers, cap


def _deferred_source_run(totals: Dict[str, int], delay: float = 0.01) -> SourceLaneRunResult:
    totals["budget_deferred"] = int(totals.get("budget_deferred", 0) or 0) + 1
    return SourceLaneRunResult(totals=totals, fatal_error=None, defer_delay_sec=float(delay))


def _source_lane_pending(owner: Any, market: str) -> bool:
    with owner._live_sniper_source_lock:
        return bool(owner._live_sniper_source_pending_by_market.get(market))


def _lane_is_current(owner: Any, market: str, epoch: int) -> bool:
    return (
        str(getattr(owner, "_active_market_key", "") or "") == str(market or "")
        and int(owner._snapshot_epoch()) == int(epoch)
    )


def _report(
    reporter: ErrorReporter, stage: str, action: str, exc: BaseException, **context: Any,
) -> None:
    if reporter is None:
        return
    try:
        reporter(stage, action, exc, **context)
    except Exception:
        # Reporting must never become a second failure in the hot path.  Keep
        # the original coordinator error authoritative while making the
        # secondary reporting failure observable.
        _LOG.exception(
            "SOURCE_PRIORITY_ERROR_REPORT_FAILED stage=%s action=%s",
            stage, action,
        )


def _requeue_selection(owner: Any, market: str, selected: SourceSelection) -> None:
    if not selected:
        return
    with owner._live_sniper_source_lock:
        pending = owner._live_sniper_source_pending_by_market.setdefault(str(market or ""), {})
        for norm, meta in selected:
            # Preserve a newer physical file version that arrived while this
            # wave was running; restore the popped version only when absent.
            pending.setdefault(str(norm), dict(meta or {}))


def _release_lease(
    owner: Any, market: str, leased: Dict[str, Dict[str, Any]], norm: Any,
    *, outcome: str = "ACKED",
) -> None:
    key = str(norm or "")
    meta = dict(leased.get(key) or {})
    if meta:
        source_lease_accounting_for_owner(owner).complete(str(market or ""), key, meta, outcome)
    leased.pop(key, None)


def _recover_open_leases(
    owner: Any, market: str, leased: Mapping[str, Mapping[str, Any]], totals: Dict[str, int],
    reporter: ErrorReporter, *, reason_code: str, stage: str = "source_parse",
    report_error: bool = True, expected_cancel: bool = False,
) -> None:
    """Return every un-ACKed selected source file to pending.

    A110 makes the SOURCE_PRIORITY wave a lease/ACK transaction: selecting a
    physical source version is not a deletion.  A file leaves the lease only
    after success, intentional stale rejection, retry staging, or deliberate
    terminal handling.  Cancel/market-switch/worker teardown therefore cannot
    silently drop the unvisited tail of a wave.
    """
    remaining = [(str(k), dict(v or {})) for k, v in dict(leased or {}).items()]
    if not remaining:
        return
    _requeue_selection(owner, market, remaining)
    _lease_registry = source_lease_accounting_for_owner(owner)
    for _norm, _meta in remaining:
        _lease_registry.complete(str(market or ""), _norm, _meta, "RECOVERED")
    totals["requeued"] = int(totals.get("requeued", 0) or 0) + len(remaining)
    totals["lease_recovered"] = int(totals.get("lease_recovered", 0) or 0) + len(remaining)
    if expected_cancel:
        totals["expected_cancel_lease_recovered"] = int(
            totals.get("expected_cancel_lease_recovered", 0) or 0
        ) + len(remaining)
    elif "CANCELLED" in str(reason_code or "").upper():
        totals["unexpected_cancel_lease_recovered"] = int(
            totals.get("unexpected_cancel_lease_recovered", 0) or 0
        ) + len(remaining)
    if report_error:
        detail = RuntimeError(str(reason_code or "SOURCE_PRIORITY_LEASE_RECOVERED"))
        for norm, meta in remaining:
            _report(
                reporter, stage, "recover_unacked_source_file_lease", detail,
                market=str(market), source_norm=str(norm),
                source_file=str((meta or {}).get("path") or ""),
                source_signature=tuple((meta or {}).get("signature") or (0, 0)),
                reason_code=str(reason_code or "SOURCE_PRIORITY_LEASE_RECOVERED"),
            )


def _requeue_resolution_selection(owner: Any, market: str, selected: SourceSelection) -> None:
    """Restore unresolved files while preserving a genuinely newer signature."""
    if not selected:
        return
    from source_generation_identity import same_physical_generation

    with owner._live_sniper_source_lock:
        pending = owner._live_sniper_source_pending_by_market.setdefault(str(market or ""), {})
        for norm, meta in selected:
            key = str(norm)
            incoming = dict(meta or {})
            current = pending.get(key)
            if current is None:
                pending[key] = incoming
                continue
            if not same_physical_generation(current, incoming):
                continue
            merged = dict(current)
            merged["source_resolution_attempts"] = max(
                int(current.get("source_resolution_attempts") or 0),
                int(incoming.get("source_resolution_attempts") or 0),
            )
            merged["source_retry_not_before_monotonic"] = max(
                float(current.get("source_retry_not_before_monotonic") or 0.0),
                float(incoming.get("source_retry_not_before_monotonic") or 0.0),
            )
            if incoming.get("source_resolution_last_reason"):
                merged["source_resolution_last_reason"] = incoming["source_resolution_last_reason"]
            pending[key] = merged


def _retry_meta(meta: Mapping[str, Any], action: Any) -> Dict[str, Any]:
    source = dict(meta or {})
    source["source_resolution_attempts"] = int(action.attempts)
    source["source_resolution_last_reason"] = str(action.reason_code or "")
    source["source_retry_not_before_monotonic"] = float(action.retry_not_before_monotonic)
    return source


def _handle_unresolved_sources(
    owner: Any, market: str, unresolved: Sequence[Any], totals: Dict[str, int],
    reporter: ErrorReporter,
) -> None:
    registry = source_file_resolution_registry(owner)
    unmapped_registry = source_unmapped_registry(owner)
    needs_universe = any(
        str(getattr(item, "reason_code", "") or "") == "SOURCE_PRIORITY_ROW_LOOKUP_EMPTY"
        for item in (unresolved or ())
    )
    universe = source_universe_snapshot(owner, market) if needs_universe else None
    retry_selection = []
    for item in unresolved or ():
        totals["unresolved"] += 1
        item_meta = dict(item.meta or {})
        item_norm = str(item.norm)
        reason_code = str(item.reason_code or "")
        generation = str(
            item_meta.get("source_universe_generation")
            or (universe.generation if universe is not None else "")
            or ""
        )
        if (
            reason_code == "SOURCE_PRIORITY_ROW_LOOKUP_EMPTY"
            and universe is not None and universe.ready
            and item_norm not in universe.norms
        ):
            unmapped_registry.record_unmapped(
                market, item_norm, generation,
                path=str(item_meta.get("path") or ""), reason_code=reason_code,
            )
            totals["ignored_unmapped"] += 1
            continue
        action = registry.record_failure(
            market, item_norm, item_meta, reason_code,
        )
        context = {
            "market": str(market),
            "source_file": str(item_meta.get("path") or ""),
            "source_norm": item_norm,
            "source_signature": tuple(item_meta.get("signature") or (0, 0)),
            "resolution_reason": reason_code,
            "resolution_attempt": int(action.attempts),
            "resolution_max_attempts": int(action.max_attempts),
        }
        detail = RuntimeError(
            f"{item.reason_code}: {item.error_type or ''}: {item.error_message or ''}".rstrip(": ")
        )
        if action.quarantined:
            totals["quarantined"] += 1
            _report(
                reporter, "source_parse", "quarantine_unresolved_source_file", detail,
                reason_code="SOURCE_PRIORITY_FILE_QUARANTINED", **context,
            )
            continue
        totals["requeued"] += 1
        retry_selection.append((item_norm, _retry_meta(item_meta, action)))
        _report(
            reporter, "source_parse", "retry_unresolved_source_file", detail,
            reason_code="SOURCE_PRIORITY_FILE_RESOLUTION_RETRY",
            retry_delay_sec=float(action.retry_delay_sec), **context,
        )
    _requeue_resolution_selection(owner, market, retry_selection)


def _source_meta_for_row(owner: Any, row: Mapping[str, Any], meta_by_norm: Mapping[str, Mapping[str, Any]]) -> Tuple[str, Dict[str, Any]]:
    filename = str((row or {}).get("filename") or (row or {}).get("path") or "")
    try:
        norm = owner._live_sniper_norm_path(filename)
    except Exception:
        norm = filename.replace("\\", "/").lower()
    meta = dict((meta_by_norm or {}).get(norm) or {})
    if not meta:
        meta = dict(row or {})
    meta.setdefault("path", filename)
    if (row or {}).get("symbol") not in (None, ""):
        meta.setdefault("symbol", str((row or {}).get("symbol") or ""))
    if (row or {}).get("name") not in (None, ""):
        meta.setdefault("name", str((row or {}).get("name") or ""))
    if (row or {}).get("fields") not in (None, ""):
        meta.setdefault("fields", (row or {}).get("fields"))
    return str(norm), meta



def _requeue_source_writer_state(
    owner: Any, market: str, row: Mapping[str, Any],
    meta_by_norm: Mapping[str, Mapping[str, Any]], totals: Dict[str, int],
    observation: Mapping[str, Any],
) -> None:
    """Requeue a normal writer-owned partial/rewrite generation without failure accounting."""
    norm, meta = _source_meta_for_row(owner, row, meta_by_norm)
    control = dict(observation or {})
    actual_signature = tuple(control.get("source_generation_signature") or meta.get("signature") or (0, 0))
    if actual_signature:
        meta["signature"] = actual_signature
    due = float(control.get("source_retry_not_before_monotonic") or 0.0)
    if due <= 0.0:
        due = time.monotonic() + max(0.01, float(control.get("source_retry_delay_sec") or 0.20))
    meta["source_retry_not_before_monotonic"] = due
    reason = str(control.get("_source_read_status") or control.get("source_read_error_code") or SOURCE_PARTIAL_WRITE).upper()
    if reason not in {SOURCE_PARTIAL_WRITE, SOURCE_REWRITE_IN_PROGRESS}:
        reason = SOURCE_PARTIAL_WRITE
    meta["source_resolution_last_reason"] = reason
    meta["source_partial_retry_attempts"] = int(control.get("source_retry_attempts") or 0)
    _requeue_resolution_selection(owner, market, [(norm, meta)])
    if reason == SOURCE_REWRITE_IN_PROGRESS:
        totals["writer_rewrites"] = int(totals.get("writer_rewrites", 0) or 0) + 1
    else:
        totals["partial_writes"] = int(totals.get("partial_writes", 0) or 0) + 1
    totals["requeued"] = int(totals.get("requeued", 0) or 0) + 1


def _retry_source_file_failure(
    owner: Any, market: str, row: Mapping[str, Any], meta_by_norm: Mapping[str, Mapping[str, Any]],
    totals: Dict[str, int], reporter: ErrorReporter, *, reason_code: str,
    exc: BaseException | None = None, stage: str = "source_parse", action_name: str = "retry_source_file_failure",
    observation: Mapping[str, Any] | None = None,
) -> None:
    """Requeue one physical source version after a transient read/process failure.

    The selected file was already leased out of the hot pending bucket.  A per-file
    failure must therefore return that exact signature to retry unless the same
    path has since received a newer signature or the signature exhausted its
    bounded attempt budget.
    """
    registry = source_file_resolution_registry(owner)
    norm, meta = _source_meta_for_row(owner, row, meta_by_norm)
    detail = exc if exc is not None else RuntimeError(str(reason_code or "SOURCE_PRIORITY_FILE_FAILED"))
    actual_reason = str(reason_code or "SOURCE_PRIORITY_FILE_FAILED")
    action = registry.record_failure(market, norm, meta, actual_reason)
    context = {
        "market": str(market),
        "symbol": str((observation or row or {}).get("symbol") or meta.get("symbol") or ""),
        "source_file": str(meta.get("path") or (row or {}).get("filename") or ""),
        "source_norm": str(norm),
        "source_signature": tuple(meta.get("signature") or (0, 0)),
        "resolution_attempt": int(action.attempts),
        "resolution_max_attempts": int(action.max_attempts),
    }
    signature = tuple(meta.get("signature") or (0, 0))
    signature_size = int(signature[1] or 0) if len(signature) > 1 else 0
    if actual_reason.upper() == "SOURCE_RECORDS_EMPTY" and signature_size <= 0:
        # The same zero-byte generation can never yield a bar.  Keep its registry
        # state so duplicate notifications are suppressed, and let a new physical
        # signature re-enter automatically.  This is not a permanent file ban.
        totals["parked_empty_placeholders"] = int(
            totals.get("parked_empty_placeholders", 0) or 0
        ) + 1
        _report(
            reporter, stage, action_name, detail,
            reason_code="SOURCE_PRIORITY_EMPTY_PLACEHOLDER_PARKED", **context,
        )
        return
    if action.quarantined:
        totals["quarantined"] += 1
        _report(
            reporter, stage, action_name, detail,
            reason_code="SOURCE_PRIORITY_FILE_QUARANTINED", **context,
        )
        return
    retry_meta = _retry_meta(meta, action)
    _requeue_resolution_selection(owner, market, [(norm, retry_meta)])
    totals["requeued"] += 1
    _report(
        reporter, stage, action_name, detail,
        reason_code=str(reason_code or "SOURCE_PRIORITY_FILE_RETRY"),
        retry_delay_sec=float(action.retry_delay_sec), **context,
    )


def _wake_source_lane_if_pending(owner: Any, market: str, epoch: int, error_reporter: ErrorReporter = None) -> None:
    try:
        if _source_lane_pending(owner, market) and _lane_is_current(owner, market, epoch) and not bool(getattr(owner, "_stop_requested", False)):
            starter = getattr(owner, "_schedule_live_sniper_source_lane", None)
            if callable(starter):
                starter(market, [], request_epoch=epoch)
    except Exception as exc:
        _report(
            error_reporter, "source_parse", "source_processing_rearm", exc, market=market,
            reason_code="SOURCE_PRIORITY_PROCESSING_REARM_FAILED",
        )


def _process_source_observation_after_read(
    owner: Any, *, market: str, epoch: int, row: Mapping[str, Any],
    meta_by_norm: Mapping[str, Mapping[str, Any]], observation: Dict[str, Any],
    leased: Dict[str, Dict[str, Any]], totals: Dict[str, Any], registry: Any,
    sniper_enabled: bool, capture_recorder: Optional[Callable[..., Any]],
    error_reporter: ErrorReporter = None,
    retry_reason_resolver: Optional[Callable[[Any], str]] = None,
) -> None:
    """Own one already-read source generation through durable ACK/requeue."""
    try:
        if sniper_enabled:
            stat = owner._process_live_sniper_source_observation(
                market, observation, epoch, lane_source="SOURCE_PRIORITY",
            )
            _merge_source_lane_stats(totals, dict(stat or {}))
            if bool((stat or {}).get("retry_required")) or not bool((stat or {}).get("source_acknowledged", True)):
                _retry_source_file_failure(
                    owner, market, row, meta_by_norm, totals, error_reporter,
                    reason_code=(
                        retry_reason_resolver(stat) if callable(retry_reason_resolver)
                        else str((stat or {}).get("retry_reason_code") or "SOURCE_OBSERVATION_DURABLE_ADMISSION_PENDING")
                    ),
                    stage="decision_write", action_name="durable_probability_admission",
                    observation=observation,
                )
                norm, _meta = _source_meta_for_row(owner, row, meta_by_norm)
                _release_lease(owner, market, leased, norm, outcome="REQUEUED")
                return
        elif capture_recorder is not None:
            accepted = capture_recorder(
                market_key=market,
                observation=observation,
                runtime_context={
                    "lane_source": "SOURCE_PRIORITY_CAPTURE_ONLY",
                    "request_epoch": epoch,
                    "active_market": str(getattr(owner, "_active_market_key", "") or ""),
                },
            )
            totals["observations"] += int(bool(accepted))
        norm, meta = _source_meta_for_row(owner, row, meta_by_norm)
        registry.record_success(market, norm, meta.get("signature"), str(meta.get("source_generation_id") or meta.get("source_audit_fingerprint") or ""))
        _release_lease(owner, market, leased, norm, outcome="ACKED")
    except Exception as exc:
        totals["errors"] += 1
        _retry_source_file_failure(
            owner, market, row, meta_by_norm, totals, error_reporter,
            reason_code="SOURCE_PRIORITY_OBSERVATION_PROCESS_FAILED", exc=exc,
            stage="cross_detection", action_name="process_live_sniper_source_observation",
            observation=observation,
        )
        norm, _meta = _source_meta_for_row(owner, row, meta_by_norm)
        _release_lease(owner, market, leased, norm, outcome="REQUEUED")


def run_source_lane_worker(
    owner: Any, *, market: str, epoch: int, cancel_event: Any, sniper_enabled: bool,
    wave_limit: Callable[[str], int], worker_count: Callable[[str], int],
    capture_recorder: Optional[Callable[..., Any]] = None,
    error_reporter: ErrorReporter = None,
) -> SourceLaneRunResult:
    """Run one bounded, recoverable SOURCE_PRIORITY wave."""
    totals = empty_source_lane_totals()

    # H12H3 provenance contract remains owned by SOURCE_PRIORITY itself.
    # H12H11 may execute the durable kernel on a separate serial processor, but
    # the worker still resolves scheduler-specific retry provenance and passes it
    # to both sync and async paths; a known reason can never collapse to generic.
    def _scheduler_retry_reason(stat: Any) -> str:
        return str((stat or {}).get("retry_reason_code") or "SOURCE_OBSERVATION_DURABLE_ADMISSION_PENDING")

    selected: SourceSelection = ()
    leased: Dict[str, Dict[str, Any]] = {}
    run_started = time.monotonic(); run_budget_sec, _workers_hint, max_files_per_run = _source_lane_run_limits(owner, market, worker_count)
    deadline = float(run_started + run_budget_sec)
    try:
        while not cancel_event.is_set():
            if (time.monotonic() - run_started) >= run_budget_sec: return _deferred_source_run(totals)
            if not _lane_is_current(owner, market, epoch):
                break
            selected = ()
            with owner._live_sniper_source_lock:
                bucket = owner._live_sniper_source_pending_by_market.get(market) or {}
                if not bucket:
                    break
                selected = order_owner_source_bucket(owner, market, bucket, min(max_files_per_run, max(1, int(wave_limit(market)))))
                if not selected:
                    # A concurrent registry/retry gate can temporarily make every
                    # pending item ineligible between bucket inspection and order.
                    # Keep ownership in pending and defer; crashing the lane here
                    # only creates noise and can amplify a harmless scheduling race.
                    retry_delay = next_source_retry_delay(bucket)
                    totals["order_empty_deferred"] = int(totals.get("order_empty_deferred", 0) or 0) + 1
                    delay = float(retry_delay) if retry_delay is not None and retry_delay > 0.0 else 0.05
                    return SourceLaneRunResult(totals=totals, fatal_error=None, defer_delay_sec=max(0.05, delay))
                requested_leases = {str(norm): dict(meta or {}) for norm, meta in selected}
                lease_registry = source_lease_accounting_for_owner(owner)
                claim_many = getattr(lease_registry, "claim_many", None)
                if callable(claim_many):
                    claim_result = dict(claim_many(market, requested_leases) or {})
                    leased = {
                        norm: meta for norm, meta in requested_leases.items()
                        if str(claim_result.get(norm) or "") == "ACQUIRED"
                    }
                    duplicate_open = [
                        norm for norm in requested_leases
                        if str(claim_result.get(norm) or "") == "ALREADY_OPEN"
                    ]
                    totals["lease_open_duplicate_suppressed"] = int(
                        totals.get("lease_open_duplicate_suppressed", 0) or 0
                    ) + len(duplicate_open)
                    # The existing open lease already owns this exact signature.
                    # Remove only this redundant pending notification; its owner will
                    # ACK/requeue the physical generation exactly once.
                    selected = [(norm, meta) for norm, meta in selected if norm in leased]
                else:
                    leased = requested_leases
                    lease_registry.acquire_many(market, leased)
                for norm in requested_leases:
                    bucket.pop(norm, None)
                if bucket:
                    owner._live_sniper_source_pending_by_market[market] = bucket
                else:
                    owner._live_sniper_source_pending_by_market.pop(market, None)
                if not selected:
                    # Every selected item was an exact generation already owned by
                    # another async lease. Avoid resolution/read duplication and let
                    # the current owner finish it.
                    continue
            _resolution_started = time.monotonic()
            resolution = resolve_selected_source_rows(owner, market, selected)
            _resolution_ms = max(0.0, (time.monotonic() - _resolution_started) * 1000.0)
            totals["source_resolution_ms_total"] = float(totals.get("source_resolution_ms_total", 0.0) or 0.0) + _resolution_ms
            totals["source_resolution_ms_max"] = max(float(totals.get("source_resolution_ms_max", 0.0) or 0.0), _resolution_ms)
            _handle_unresolved_sources(
                owner, market, resolution.unresolved, totals, error_reporter,
            )
            for item in (resolution.unresolved or ()):  # retry/quarantine handler now owns these file leases.
                _release_lease(owner, market, leased, getattr(item, "norm", ""), outcome="REQUEUED")
            # H12H11: resolution is still synchronous, but it can no longer silently
            # consume the rest of the wave and then launch more reads.  Once the hard
            # budget is exhausted, every still-owned resolved generation is returned
            # to pending before any additional work starts.
            if time.monotonic() >= deadline:
                totals["source_resolution_budget_exhausted"] = int(totals.get("source_resolution_budget_exhausted", 0) or 0) + 1
                _remaining = [(norm, dict(meta or {})) for norm, meta in list(leased.items())]
                if _remaining:
                    _requeue_selection(owner, market, _remaining)
                    for _norm, _meta in list(_remaining):
                        _release_lease(owner, market, leased, _norm, outcome="BUDGET_DEFERRED")
                totals["budget_deferred"] = int(totals.get("budget_deferred", 0) or 0) + int(len(_remaining) or 1)
                return SourceLaneRunResult(totals=totals, fatal_error=None, defer_delay_sec=0.01)
            rows = [dict(row) for row in resolution.rows]
            meta_by_norm = dict(resolution.meta_by_norm)
            if not rows:
                _recover_open_leases(
                    owner, market, leased, totals, error_reporter,
                    reason_code="SOURCE_PRIORITY_SELECTED_ROWS_EMPTY_LEASE_RECOVERY",
                )
                leased = {}
                selected = ()
                continue
            registry = source_file_resolution_registry(owner)
            workers = min(max(1, int(worker_count(market))), len(rows))
            pool = ThreadPoolExecutor(
                max_workers=workers, thread_name_prefix="AinLiveSniperRead",
            )
            future_meta = {}
            for row in rows:
                norm = owner._live_sniper_norm_path(row.get("filename"))
                meta = meta_by_norm.get(norm) or {}
                future = pool.submit(
                    owner._read_live_sniper_source_observation,
                    market, row, meta.get("detected_at"),
                )
                future_meta[future] = row

            def _handle_completed_future(future):
                nonlocal leased
                if cancel_event.is_set():
                    expected_cancel = bool(
                        getattr(owner, "_stop_requested", False)
                        or not bool(getattr(owner, "_live_poll_enabled", False))
                        or not _lane_is_current(owner, market, epoch)
                    )
                    _recover_open_leases(
                        owner, market, leased, totals, error_reporter,
                        reason_code=(
                            "SOURCE_PRIORITY_EXPECTED_SHUTDOWN_LEASE_RECOVERY"
                            if expected_cancel
                            else "SOURCE_PRIORITY_WORKER_CANCELLED_WITH_OPEN_LEASES"
                        ),
                        report_error=not expected_cancel,
                        expected_cancel=expected_cancel,
                    )
                    leased = {}
                    return
                row = dict(future_meta.get(future) or {})
                totals["files"] += 1
                try:
                    observation = future.result()
                except Exception as exc:
                    totals["errors"] += 1
                    _retry_source_file_failure(
                        owner, market, row, meta_by_norm, totals, error_reporter,
                        reason_code="SOURCE_PRIORITY_FILE_READ_FAILED", exc=exc,
                        stage="source_parse", action_name="read_live_sniper_source_file",
                    )
                    norm, _meta = _source_meta_for_row(owner, row, meta_by_norm)
                    _release_lease(owner, market, leased, norm, outcome="REQUEUED")
                    return
                if not isinstance(observation, dict):
                    totals["errors"] += 1
                    _retry_source_file_failure(
                        owner, market, row, meta_by_norm, totals, error_reporter,
                        reason_code="SOURCE_PRIORITY_FILE_READ_NON_MAPPING",
                        stage="source_parse", action_name="read_live_sniper_source_file",
                    )
                    norm, _meta = _source_meta_for_row(owner, row, meta_by_norm)
                    _release_lease(owner, market, leased, norm, outcome="REQUEUED")
                    return
                _read_code = str(
                    observation.get("_source_read_status")
                    or observation.get("source_read_error_code")
                    or observation.get("source_read_reason_code")
                    or observation.get("_source_read_reason")
                    or ""
                ).upper()
                if (
                    bool(observation.get("_source_read_writer_busy"))
                    or bool(observation.get("_source_read_partial"))
                    or _read_code in {SOURCE_PARTIAL_WRITE, SOURCE_REWRITE_IN_PROGRESS}
                ):
                    _requeue_source_writer_state(
                        owner, market, row, meta_by_norm, totals, observation,
                    )
                    norm, _meta = _source_meta_for_row(owner, row, meta_by_norm)
                    _release_lease(owner, market, leased, norm, outcome="REQUEUED")
                    return
                if bool(observation.get("_source_read_error")):
                    totals["errors"] += 1
                    _retry_source_file_failure(
                        owner, market, row, meta_by_norm, totals, error_reporter,
                        reason_code=_read_code or "SOURCE_PRIORITY_FILE_READ_ERROR",
                        stage="source_parse", action_name="read_live_sniper_source_file",
                        observation=observation,
                    )
                    norm, _meta = _source_meta_for_row(owner, row, meta_by_norm)
                    _release_lease(owner, market, leased, norm, outcome="REQUEUED")
                    return
                freshness = assess_live_source_bar(observation, market_key=market)
                # H12H12 convergence: `allowed` is the authority bit.  The other
                # FreshnessDecision fields are diagnostics and must never crash the
                # source lane if an adapter/test/provider omits one.  Missing/false
                # authority remains fail-closed.
                freshness_allowed = bool(getattr(freshness, "allowed", False))
                freshness_reason = str(
                    getattr(freshness, "reason_code", "")
                    or ("LIVE_SOURCE_ALLOWED" if freshness_allowed else "LIVE_SOURCE_REJECTED_UNSPECIFIED")
                )
                freshness_market_date = str(getattr(freshness, "market_date", "") or "")
                freshness_bar_date = str(getattr(freshness, "bar_date", "") or "")
                freshness_age_sec = getattr(freshness, "age_sec", None)
                freshness_maximum_lag_sec = getattr(freshness, "maximum_lag_sec", None)
                observation.update({
                    "source_freshness_reason_code": freshness_reason,
                    "source_market_date": freshness_market_date,
                    "source_bar_date": freshness_bar_date,
                    "source_freshness_age_sec": freshness_age_sec,
                    "source_freshness_maximum_lag_sec": freshness_maximum_lag_sec,
                })
                if not freshness_allowed:
                    observation.update({
                        "_source_content_stale": True,
                    })
                    totals["stale_bars_rejected"] += 1
                    norm, meta = _source_meta_for_row(owner, row, meta_by_norm)
                    registry.record_success(market, norm, meta.get("signature"), str(meta.get("source_generation_id") or meta.get("source_audit_fingerprint") or ""))
                    _release_lease(owner, market, leased, norm, outcome="STALE_REJECTED")
                    if capture_recorder is not None:
                        capture_recorder(
                            market_key=market, observation=observation,
                            runtime_context={
                                "lane_source": "SOURCE_PRIORITY_STALE_AUDIT",
                                "request_epoch": epoch,
                                "source_freshness_reason_code": freshness_reason,
                            },
                        )
                    return
                # H12H11: SOURCE_PRIORITY owns discovery/read/freshness only.  The
                # durable observation kernel is serialized in a separate fair worker
                # so a 20-60s downstream decision path cannot stretch a 2s source wave.
                _async_processing = bool(getattr(owner, "_live_source_processing_async_enabled", False))
                if _async_processing:
                    norm, meta = _source_meta_for_row(owner, row, meta_by_norm)
                    lease_meta = dict(leased.pop(str(norm), None) or meta or {})
                    enqueued = time.monotonic()
                    observation["source_processing_enqueued_monotonic"] = float(enqueued)
                    observation["source_processing_enqueued_at_utc"] = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
                    # Classify current-observation decision content before the
                    # downstream scheduler bucket is frozen.  Historical owner/pulse
                    # state alone cannot know that a brand-new source generation is
                    # the first cross; in the US live court those Births could sit
                    # behind hundreds of neutral startup-rescan tasks.
                    _score_kind = str(observation.get("live_sniper_score_kind") or "").strip().upper()
                    _decision_bearing_hint = bool(
                        observation.get("positive_cross_now")
                        or observation.get("negative_cross_now")
                        or observation.get("seal_score_request")
                        or _score_kind in {"SEAL", "R1", "R50", "R100", "STOP"}
                    )
                    try:
                        bucket = int(source_attention_bucket_for_owner(owner, market, meta))
                    except Exception:
                        bucket = 3
                    if _decision_bearing_hint:
                        # R1/terminal target/stop/seal work is the most time-sensitive
                        # source truth. New positive/negative crosses use the active
                        # cross bucket. Fairness remains enforced by the scheduler.
                        if _score_kind in {"R1", "R50", "R100", "STOP", "SEAL"}:
                            bucket = min(bucket, 0)
                        else:
                            bucket = min(bucket, 1)

                    def _run_processing(queue_wait_ms, _obs=dict(observation), _row=dict(row), _meta=dict(meta), _lease_meta=lease_meta, _norm=str(norm)):
                        # The task may sit behind another authoritative observation.
                        # Re-check epoch immediately before durable processing so a
                        # market switch cannot let queued work from the old session
                        # mutate the new market.  Cancellation owns the lease and does
                        # not requeue when the epoch is no longer current.
                        if bool(getattr(owner, "_stop_requested", False)) or not _lane_is_current(owner, market, epoch):
                            _cancel_processing("STALE_SOURCE_PROCESSING_EPOCH")
                            return
                        task_totals = empty_source_lane_totals()
                        _obs["source_processing_started_at_utc"] = __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()
                        _obs["source_processing_queue_wait_ms"] = float(queue_wait_ms)
                        _obs["source_engine_observed_at_utc"] = _obs["source_processing_started_at_utc"]
                        one_lease = {_norm: dict(_lease_meta)}
                        task_started = time.monotonic()
                        _process_source_observation_after_read(
                            owner, market=market, epoch=epoch, row=_row,
                            meta_by_norm={_norm: dict(_meta)}, observation=_obs,
                            leased=one_lease, totals=task_totals, registry=registry,
                            sniper_enabled=sniper_enabled, capture_recorder=capture_recorder,
                            error_reporter=error_reporter, retry_reason_resolver=_scheduler_retry_reason,
                        )
                        _obs["source_processing_duration_ms"] = float((time.monotonic() - task_started) * 1000.0)
                        _wake_source_lane_if_pending(owner, market, epoch, error_reporter)

                    def _cancel_processing(reason, _norm=str(norm), _meta=lease_meta):
                        reason_text = str(reason or "CANCELLED")
                        try:
                            source_lease_accounting_for_owner(owner).complete(str(market or ""), _norm, dict(_meta or {}), reason_text)
                        finally:
                            # A newer pending task for the same physical path already
                            # owns the latest generation. Requeueing the superseded
                            # generation recreates the H12H13 storm amplification.
                            if (
                                not reason_text.startswith("SUPERSEDED_BY_NEWER_SOURCE_TASK")
                                and not bool(getattr(owner, "_stop_requested", False))
                                and _lane_is_current(owner, market, epoch)
                            ):
                                _requeue_selection(owner, market, [(_norm, dict(_meta or {}))])

                    # H12H14 no-loss coalescing: only neutral observations for a
                    # source with no known active episode are eligible for pending
                    # supersession. A decision-bearing generation (cross/score/seal)
                    # opens a scheduler barrier so later generations for the same DAT
                    # are preserved until the chain drains.
                    _active_episode_hint = False
                    _neutrality_proven = False
                    if not _decision_bearing_hint:
                        # H12H14H1 Windows live preflight: never cold-import the heavy
                        # pulse candidate module inside the bounded SOURCE_PRIORITY
                        # coordinator.  On a cold process, importing pulse_tick_tape can
                        # take hundreds of milliseconds under Windows filesystem/AV
                        # scheduling and therefore violates the source-wave deadline even
                        # though durable processing is correctly asynchronous.
                        #
                        # Absence of an already-loaded authoritative probe is handled
                        # fail-safe: preserve this physical generation (disable
                        # coalescing) rather than guessing that the source is neutral.
                        # The downstream durable processor is outside the source-wave
                        # budget and remains free to import/use pulse_tick_tape normally.
                        _pulse_tick_tape_module = sys.modules.get("pulse_tick_tape")
                        if _pulse_tick_tape_module is None:
                            _active_episode_hint = True
                            totals["coalesce_probe_unavailable_preserved"] = int(
                                totals.get("coalesce_probe_unavailable_preserved", 0) or 0
                            ) + 1
                        else:
                            try:
                                _candidate_probe = getattr(
                                    _pulse_tick_tape_module, "has_active_candidate_for_symbol", None
                                )
                                if callable(_candidate_probe):
                                    _active_episode_hint = bool(
                                        _candidate_probe(
                                            str(market),
                                            str(observation.get("symbol") or "").strip().upper(),
                                        )
                                    )
                                    _neutrality_proven = bool(not _active_episode_hint)
                                else:
                                    # A loaded legacy/stub module without the authority
                                    # probe is not proof of neutrality. Preserve work.
                                    _active_episode_hint = True
                                    totals["coalesce_probe_unavailable_preserved"] = int(
                                        totals.get("coalesce_probe_unavailable_preserved", 0) or 0
                                    ) + 1
                            except Exception as exc:
                                # Fail safe: inability to prove the source neutral disables
                                # coalescing for this task; it must be processed normally.
                                _active_episode_hint = True
                                _report(
                                    error_reporter, "source_parse", "source_processing_coalesce_scope", exc,
                                    market=market, path=str(norm),
                                    reason_code="SOURCE_PROCESSING_COALESCE_SCOPE_FAILED_SAFE",
                                )
                    _coalescible = bool(not _decision_bearing_hint and not _active_episode_hint)
                    _capacity_workers, _capacity_reserve = source_processing_capacity_hints(
                        str(market), str(norm),
                        str(observation.get("source_kind") or observation.get("source") or row.get("source_kind") or ""),
                        saudi_capacity_enabled=True,
                    )

                    task = SourceProcessingTask(
                        market=str(market), norm=str(norm), bucket=bucket,
                        enqueued_monotonic=float(enqueued), run=_run_processing, cancel=_cancel_processing,
                        generation_seq=int(observation.get("source_mtime_ns") or observation.get("source_generation_seq") or -1),
                        generation_identity=str(observation.get("source_generation_id") or observation.get("source_snapshot_id") or observation.get("source_tail_sha256") or ""),
                        coalescible=_coalescible,
                        barrier_coalescible=bool(_coalescible and _neutrality_proven),
                        symbol=str(observation.get("symbol") or "").strip().upper(),
                        parallelism_hint=int(_capacity_workers),
                        protected_reserve_hint=int(_capacity_reserve),
                    )
                    if source_processing_scheduler_for_owner(owner).enqueue(task):
                        totals["processing_enqueued"] = int(totals.get("processing_enqueued", 0) or 0) + 1
                        return
                    totals["processing_queue_full"] = int(totals.get("processing_queue_full", 0) or 0) + 1
                    _requeue_selection(owner, market, [(str(norm), dict(meta or {}))])
                    source_lease_accounting_for_owner(owner).complete(str(market or ""), str(norm), dict(lease_meta or {}), "PROCESS_QUEUE_FULL_REQUEUED")
                    return

                _process_source_observation_after_read(
                    owner, market=market, epoch=epoch, row=row, meta_by_norm=meta_by_norm,
                    observation=observation, leased=leased, totals=totals, registry=registry,
                    sniper_enabled=sniper_enabled, capture_recorder=capture_recorder,
                    error_reporter=error_reporter, retry_reason_resolver=_scheduler_retry_reason,
                )

            pending_futures = set(future_meta)
            deadline_hit = False
            try:
                while pending_futures:
                    remaining = float(deadline - time.monotonic())
                    if remaining <= 0.0:
                        deadline_hit = True
                        break
                    done_now, pending_futures = wait(
                        pending_futures, timeout=remaining, return_when=FIRST_COMPLETED,
                    )
                    if not done_now:
                        deadline_hit = True
                        break
                    for future in done_now:
                        _handle_completed_future(future)
                        if time.monotonic() >= deadline:
                            deadline_hit = bool(pending_futures)
                            break
                    if deadline_hit:
                        break

                if pending_futures:
                    deadline_hit = True
                    totals["budget_deferred"] = int(totals.get("budget_deferred", 0) or 0) + int(len(pending_futures))
                    for future in list(pending_futures):
                        row = dict(future_meta.get(future) or {})
                        norm, meta = _source_meta_for_row(owner, row, meta_by_norm)
                        if future.cancel():
                            _requeue_selection(owner, market, [(norm, dict(meta or {}))])
                            _release_lease(owner, market, leased, norm, outcome="REQUEUED")
                            continue
                        # A running read must not hold the SOURCE_PRIORITY coordinator
                        # beyond its hard budget.  Keep its lease owned until the read
                        # finishes, then requeue the physical generation (or preserve a
                        # newer already-pending generation) and release accounting.
                        late_meta = dict(leased.pop(str(norm), None) or meta or {})
                        _deferred_read_registered(owner)
                        def _late_done(fut, _norm=str(norm), _meta=late_meta):
                            callback_error = False
                            try:
                                try:
                                    fut.result()
                                except Exception as exc:
                                    callback_error = True
                                    _report(
                                        error_reporter, "source_parse", "deadline_deferred_read_complete", exc,
                                        market=market, path=_norm,
                                        reason_code="SOURCE_PRIORITY_DEADLINE_READ_FAILED",
                                    )
                                try:
                                    source_lease_accounting_for_owner(owner).complete(str(market or ""), _norm, _meta, "DEADLINE_DEFERRED")
                                except Exception as exc:
                                    callback_error = True
                                    _report(error_reporter, "source_parse", "deadline_deferred_lease_complete", exc, market=market, path=_norm, reason_code="SOURCE_PRIORITY_DEADLINE_LEASE_COMPLETE_FAILED")
                                _requeue_selection(owner, market, [(_norm, _meta)])
                                try:
                                    if _lane_is_current(owner, market, epoch) and not bool(getattr(owner, "_stop_requested", False)):
                                        starter = getattr(owner, "_schedule_live_sniper_source_lane", None)
                                        if callable(starter):
                                            starter(market, [], request_epoch=epoch)
                                except Exception as exc:
                                    callback_error = True
                                    _report(error_reporter, "source_parse", "deadline_deferred_reschedule", exc, market=market, path=_norm, reason_code="SOURCE_PRIORITY_DEADLINE_RESCHEDULE_FAILED")
                            finally:
                                _deferred_read_completed(owner, callback_error=callback_error)
                        future.add_done_callback(_late_done)
            finally:
                pool.shutdown(wait=False, cancel_futures=True)
            _recover_open_leases(
                owner, market, leased, totals, error_reporter,
                reason_code="SOURCE_PRIORITY_WAVE_COMPLETED_WITH_OPEN_LEASES",
            )
            leased = {}
            if deadline_hit:
                return SourceLaneRunResult(totals=totals, fatal_error=None, defer_delay_sec=0.01)
            selected = ()
            if _source_lane_pending(owner, market): return _deferred_source_run(totals)
            break
    except Exception as exc:
        totals["errors"] += 1
        if leased:
            _recover_open_leases(
                owner, market, leased, totals, error_reporter,
                reason_code="SOURCE_PRIORITY_FATAL_WITH_OPEN_LEASES",
            )
        else:
            _requeue_selection(owner, market, selected)
        return SourceLaneRunResult(totals=totals, fatal_error=exc)
    return SourceLaneRunResult(totals=totals, fatal_error=None)


__all__ = [
    "VERSION", "SourceLaneRunResult", "empty_source_lane_totals", "run_source_lane_worker",
    "quiesce_owner_deferred_source_reads", "source_runtime_shutdown_snapshot",
]
