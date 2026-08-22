# -*- coding: utf-8 -*-
"""V86CL R168 — execution-truth research recorder.

The recorder preserves every distinct source-file observation accepted by the
live lanes without blocking the sniper. JSONL WAL is authoritative. Parquet is
an asynchronous, fixed-schema research projection.

Durability is explicit rather than implied:
    accepted/enqueued -> WAL written -> WAL fsynced
The stats file exposes the current gap between those states. An abrupt process
or power loss can lose rows that have not reached WAL/fsync; this is measured
and never described as zero-loss.
"""
from __future__ import annotations
from exception_observability import report_suppressed_exception as _report_suppressed_exception

import atexit
import datetime as dt
import hashlib
import importlib.util
import json
import logging
import math
import os
import queue
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from atomic_io_utils import (
    DURABLE_STATE_REPLACE_DELAYS, replace_with_retry, write_json_atomic,
    sync_file, sync_parent_directory,
)
from market_key_contract import canonical_runtime_key
from market_datetime_normalizer import (
    canonical_market_session_date as _canonical_market_session_date,
    source_bar_to_market_naive as _source_bar_to_market_naive,
)
from probability_audit_contract import (
    DETECTOR_FAMILY, TIMEFRAME, episode_identity, trace_identity, rejection_fields,
)
from probability_performance_contract import (
    TIMING_FIELDS as _PROBABILITY_TIMING_FIELDS,
    finalize_timing as _finalize_probability_timing,
    timing_row_complete as _probability_timing_row_complete,
)
from runtime_session import current_session_id, error_stats as runtime_error_stats, get_runtime, log_event, record_stage_error
from column_truth_contract import (
    apply_trader_column_truth as _truth_apply_trader_columns,
    annotate_sealed_close_claim as _truth_annotate_sealed_close_claim,
    verified_sealed_close as _truth_verified_sealed_close,
    wall_time_key as _truth_wall_time_key,
)

try:
    from market_key_contract import market_family as _mk_family
except Exception:
    def _mk_family(_value): return "generic"

def _canonical_market_family(value: Any) -> str:
    fam = _mk_family(value)
    return {"sa": "saudi", "us": "us", "fx": "fx"}.get(fam, "generic")

VERSION = "A4_2_9_FALLBACK_STALE_AUDIT_PROVENANCE_RECORDER"
CAUSAL_AUDIT_VERSION = "A95_CAUSAL_AUDIT_RECORDER_V1"
SCHEMA_VERSION = 11
SCHEMA_FILENAME = "FULL_TICK_RESEARCH_SCHEMA_V11.json"
_DATASET_BY_KIND = {
    "SOURCE_OBSERVATION": "source_observations",
    "DECISION_ENRICHMENT": "decision_enrichments",
    "EVENT_UPDATE": "event_updates",
    "DECISION_CONTEXT_SNAPSHOT": "decision_context_snapshots",
}


def _env_bool(name: str, default: bool) -> bool:
    value = str(os.environ.get(name, "1" if default else "0")).strip().lower()
    return value not in {"0", "false", "no", "off"}


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)) or default)
    except Exception:
        value = default
    return max(lo, min(hi, value))


def _env_float(name: str, default: float, lo: float, hi: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)) or default)
    except Exception:
        value = default
    return max(lo, min(hi, value))


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _elapsed_ms(start: Any, end: Any = None) -> Optional[float]:
    try:
        if isinstance(start, (int, float)):
            start_s = float(start)
        else:
            start_s = dt.datetime.fromisoformat(str(start).replace("Z", "+00:00")).timestamp()
        if end is None:
            end_s = time.time()
        elif isinstance(end, (int, float)):
            end_s = float(end)
        else:
            end_s = dt.datetime.fromisoformat(str(end).replace("Z", "+00:00")).timestamp()
        return max(0.0, (end_s - start_s) * 1000.0)
    except Exception:
        return None


def _slug(value: Any) -> str:
    text = str(value or "market").strip().lower()
    out = "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")
    return out or "market"


def _date_text(value: Any, fallback: Optional[str] = None) -> str:
    text = str(value or "").strip()
    if len(text) >= 10 and text[4:5] == "-" and text[7:8] == "-":
        return text[:10]
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return fallback or dt.datetime.now().date().isoformat()


def _canonical_session_date_for_row(row: Mapping[str, Any], market_key: Any = "") -> str:
    source = dict(row or {})
    explicit_text = str(
        source.get("canonical_market_session_date") or source.get("market_session_date") or ""
    ).strip()
    explicit = explicit_text[:10] if len(explicit_text) >= 10 and explicit_text[4:5] == "-" and explicit_text[7:8] == "-" else ""
    if explicit:
        return explicit
    market = canonical_runtime_key(market_key or source.get("market_key") or source.get("canonical_market_key"))
    bar = (
        source.get("episode_signal_bar_time") or source.get("signal_bar_time")
        or source.get("bar_datetime") or source.get("bar_date")
    )
    observed = (
        source.get("observed_at") or source.get("source_observed_at")
        or source.get("source_detected_at") or source.get("engine_received_at_utc")
    )
    canonical = _canonical_market_session_date(bar, market_key=market, observed_at=observed)
    return canonical or _date_text(source.get("session_date") or bar)


def _hour_text(value: Any) -> str:
    text = str(value or "").strip()
    try:
        return f"{dt.datetime.fromisoformat(text.replace('Z', '+00:00')).hour:02d}"
    except Exception as _suppressed_exc:
        _report_suppressed_exception(
            _suppressed_exc, module=__name__, file=__file__,
            function='_hour_text', line=141,
            stage='wal_write', critical=True,
        )
    if len(text) >= 13 and text[10:11] in {"T", " "} and text[11:13].isdigit():
        return text[11:13]
    return "00"


def _simple_value(value: Any, *, _depth: int, _seen: set[int]) -> Any:
    if _depth > 32:
        return "<MAX_DEPTH>"
    if value is None:
        return None
    if isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    item_method = getattr(value, "item", None)
    if callable(item_method):
        try:
            item = item_method()
            if item is not value:
                return _simple_value(item, _depth=_depth + 1, _seen=_seen)
        except Exception as exc:
            _report_suppressed_exception(
                exc, module=__name__, file=__file__, function="_simple_value", line=0,
                stage="wal_write", critical=True,
            )
    if isinstance(value, Mapping):
        identity = id(value)
        if identity in _seen:
            return "<CYCLE>"
        _seen.add(identity)
        try:
            return {str(k): _simple_value(v, _depth=_depth + 1, _seen=_seen) for k, v in value.items()}
        finally:
            _seen.discard(identity)
    if isinstance(value, (list, tuple, set)):
        identity = id(value)
        if identity in _seen:
            return "<CYCLE>"
        _seen.add(identity)
        try:
            return [_simple_value(v, _depth=_depth + 1, _seen=_seen) for v in value]
        finally:
            _seen.discard(identity)
    return str(value)


def _simple(value: Any, *, _depth: int = 0, _seen: Optional[set[int]] = None) -> Any:
    """Normalize a WAL scalar without recursion crashes on hostile objects."""
    normalized = _simple_value(value, _depth=_depth, _seen=_seen if _seen is not None else set())
    if isinstance(value, Mapping):
        return json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    if isinstance(value, (list, tuple, set)):
        return json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return normalized


def _normalize_mapping(mapping: Optional[Mapping[str, Any]], *, excluded: Iterable[str] = ()) -> Dict[str, Any]:
    excluded_set = set(excluded)
    out: Dict[str, Any] = {}
    for key, value in dict(mapping or {}).items():
        key_text = str(key)
        if key_text in excluded_set or key_text.startswith("_"):
            continue
        out[key_text] = _simple(value)
    return out


def parquet_engine_name() -> str:
    # PyArrow is the only official research writer because it preserves the
    # declared Arrow schema and metadata fingerprint across every part.
    return "pyarrow" if importlib.util.find_spec("pyarrow") else ""


def parquet_engine_available() -> bool:
    return bool(parquet_engine_name())


def _process_start_time(pid: int) -> Optional[float]:
    try:
        import psutil  # type: ignore

        return float(psutil.Process(int(pid)).create_time())
    except Exception:
        return None


def _process_matches(pid: int, expected_start: Optional[float]) -> Optional[bool]:
    """Return True/False when known, None when the platform cannot decide."""
    if pid <= 0:
        return False
    try:
        import psutil  # type: ignore

        proc = psutil.Process(int(pid))
        if not proc.is_running():
            return False
        try:
            if str(proc.status()).lower() == "zombie":
                return False
        except Exception as _suppressed_exc:
            _report_suppressed_exception(
                _suppressed_exc, module=__name__, file=__file__,
                function='_process_matches', line=214,
                stage='wal_write', critical=True,
            )
        if expected_start is not None:
            try:
                if abs(float(proc.create_time()) - float(expected_start)) > 1.0:
                    return False
            except Exception:
                return None
        return True
    except Exception as exc:
        if exc.__class__.__name__ in {"NoSuchProcess", "ZombieProcess"}:
            return False
    try:
        os.kill(int(pid), 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return None


_SCHEMA_CACHE: Optional[Dict[str, Any]] = None


def _schema_contract() -> Dict[str, Any]:
    global _SCHEMA_CACHE
    if _SCHEMA_CACHE is None:
        path = Path(__file__).resolve().parent / SCHEMA_FILENAME
        _SCHEMA_CACHE = json.loads(path.read_text(encoding="utf-8"))
    return _SCHEMA_CACHE


def _dataset_for_row(row: Mapping[str, Any]) -> str:
    explicit = str(row.get("dataset_name") or "").strip()
    if explicit:
        return explicit
    return _DATASET_BY_KIND.get(str(row.get("record_kind") or "").upper(), "event_updates")


def _dataset_columns(dataset: str) -> List[Dict[str, str]]:
    datasets = dict(_schema_contract().get("datasets") or {})
    if dataset not in datasets:
        raise KeyError(f"unknown research dataset: {dataset}")
    return list(datasets[dataset].get("columns") or [])


def _schema_fingerprint(dataset: str) -> str:
    payload = json.dumps(_dataset_columns(dataset), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _coerce(value: Any, type_name: str) -> Any:
    if value is None or value == "":
        return None
    if type_name == "string":
        return _simple(value) if isinstance(_simple(value), str) else str(_simple(value))
    if type_name == "bool":
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {"1", "true", "yes", "on"}:
            return True
        if text in {"0", "false", "no", "off"}:
            return False
        return None
    if type_name == "int64":
        try:
            return int(float(value))
        except Exception:
            return None
    if type_name == "float64":
        try:
            number = float(value)
            return number if math.isfinite(number) else None
        except Exception:
            return None
    return _simple(value)


def normalize_row_for_dataset(row: Mapping[str, Any], dataset: str) -> Dict[str, Any]:
    columns = _dataset_columns(dataset)
    names = {str(col["name"]) for col in columns}
    extras: Dict[str, Any] = {}
    existing_extra = row.get("extra_json")
    if existing_extra:
        try:
            parsed = json.loads(str(existing_extra))
            if isinstance(parsed, dict):
                extras.update(parsed)
        except Exception:
            extras["legacy_extra_json"] = str(existing_extra)
    for key, value in row.items():
        if key not in names and value is not None:
            extras[str(key)] = _simple(value)
    out: Dict[str, Any] = {}
    for col in columns:
        name = str(col["name"])
        if name == "extra_json":
            out[name] = json.dumps(extras, ensure_ascii=False, sort_keys=True, separators=(",", ":")) if extras else None
        elif name == "schema_version":
            out[name] = SCHEMA_VERSION
        elif name == "dataset_name":
            out[name] = dataset
        elif name == "symbol_source_seq_scope" and not row.get(name):
            out[name] = "RECORDER_SESSION_LEGACY"
        else:
            out[name] = _coerce(row.get(name), str(col["type"]))
    return out


@dataclass
class _Segment:
    partition: Tuple[str, str, str, str]
    open_path: Path
    handle: Any
    rows: int
    bytes_written: int
    opened_at: float
    last_flush_at: float
    last_fsync_at: float
    sequence: int
    unfsynced_seqs: List[int] = field(default_factory=list)


def _a95_probability_record_fields(probability: Mapping[str, Any]) -> Dict[str, Any]:
    prob = dict(probability or {})
    names = list(prob.get("probability_feature_names") or [])
    values = dict(prob.get("probability_feature_values") or {})
    names_json = str(prob.get("probability_feature_names_json") or "")
    values_json = str(prob.get("probability_feature_values_json") or "")
    if not names_json and names:
        names_json = json.dumps(_simple(names), ensure_ascii=False, separators=(",", ":"))
    if not values_json and values:
        values_json = json.dumps(_simple(values), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "probability_score_stage": prob.get("probability_score_stage") or prob.get("score_stage"),
        "score_stage": prob.get("score_stage") or prob.get("probability_score_stage"),
        "calibration_status": prob.get("calibration_status"),
        "probability_anchor_type": prob.get("probability_anchor_type"),
        "training_anchor_contract": prob.get("training_anchor_contract"),
        "probability_feature_names_json": names_json,
        "probability_feature_values_json": values_json,
        "probability_feature_count_received": prob.get("probability_feature_count_received"),
        "probability_market_snapshot_key": prob.get("probability_market_snapshot_key") or prob.get("probability_snapshot_cache_key"),
        "probability_market_snapshot_id": prob.get("probability_market_snapshot_id") or prob.get("probability_market_snapshot_key"),
        "probability_market_snapshot_asof": prob.get("probability_market_snapshot_asof"),
        "probability_market_target_bar_time": prob.get("probability_market_target_bar_time") or prob.get("market_target_bar_time"),
        "probability_market_snapshot_symbols": prob.get("probability_market_snapshot_symbols") or prob.get("market_snapshot_symbols"),
        "probability_market_target_bar_symbols": prob.get("probability_market_target_bar_symbols") or prob.get("market_target_bar_symbols"),
        "probability_market_coverage_ratio": prob.get("probability_market_coverage_ratio"),
        "probability_score_started_at": prob.get("probability_score_started_at"),
        "probability_score_finished_at": prob.get("probability_score_finished_at"),
        "causal_audit_schema_version": prob.get("causal_audit_schema_version"),
        "causal_audit_contract_version": prob.get("causal_audit_contract_version"),
        "causal_audit_request_id": prob.get("causal_audit_request_id"),
        "causal_audit_snapshot_key": prob.get("causal_audit_snapshot_key"),
        "causal_audit_request_payload_sha256": prob.get("causal_audit_request_payload_sha256"),
        "causal_audit_snapshot_capture_enqueued": prob.get("causal_audit_snapshot_capture_enqueued"),
        "causal_audit_request_capture_enqueued": prob.get("causal_audit_request_capture_enqueued"),
        "causal_audit_result_capture_enqueued": prob.get("causal_audit_result_capture_enqueued"),
        "calibrator_sha256": prob.get("calibrator_sha256") or prob.get("probability_calibrator_sha256"),
        "r50_calibrator_sha256": prob.get("r50_calibrator_sha256") or prob.get("probability_r50_calibrator_sha256"),
        "r100_calibrator_sha256": prob.get("r100_calibrator_sha256") or prob.get("probability_r100_calibrator_sha256"),
    }


class FullSourceObservationRecorder:
    """Thread-safe recorder with WAL, leases, recovery, and fixed-schema Parquet."""

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        *,
        enabled: Optional[bool] = None,
        queue_max: Optional[int] = None,
        segment_rows: Optional[int] = None,
        segment_bytes: Optional[int] = None,
        segment_seconds: Optional[float] = None,
        parquet_writer: Optional[Callable[[Path, Path], Tuple[int, int]]] = None,
        start_immediately: bool = False,
        session_id: Optional[str] = None,
    ) -> None:
        self.enabled = _env_bool("AIN_FULL_TICK_RECORDER_ENABLED", True) if enabled is None else bool(enabled)
        root_env = str(os.environ.get("AIN_FULL_TICK_RECORDER_DIR", "")).strip()
        self.base_dir = Path(base_dir or root_env or (Path(__file__).resolve().parent / "datainfo" / "live_tick_research"))
        self.wal_root = self.base_dir / "wal"
        self.parquet_root = self.base_dir / "parquet"
        self.stats_root = self.base_dir / "stats"
        self.lock_root = self.base_dir / "locks"
        self.queue_max = int(queue_max or _env_int("AIN_FULL_TICK_QUEUE_MAX", 65536, 4096, 2_000_000))
        self.segment_rows = int(segment_rows or _env_int("AIN_FULL_TICK_SEGMENT_ROWS", 25000, 100, 1_000_000))
        self.segment_bytes = int(segment_bytes or _env_int("AIN_FULL_TICK_SEGMENT_BYTES", 64 * 1024 * 1024, 1_000_000, 2_000_000_000))
        self.segment_seconds = float(segment_seconds or _env_float("AIN_FULL_TICK_SEGMENT_SECONDS", 300.0, 5.0, 3600.0))
        self.flush_seconds = _env_float("AIN_FULL_TICK_FLUSH_SECONDS", 0.50, 0.05, 30.0)
        self.fsync_seconds = _env_float("AIN_FULL_TICK_FSYNC_SECONDS", 3.0, 0.25, 120.0)
        self.recovery_scan_seconds = _env_float("AIN_FULL_TICK_RECOVERY_SCAN_SECONDS", 30.0, 1.0, 600.0)
        self.unknown_owner_grace_seconds = _env_float("AIN_FULL_TICK_UNKNOWN_OWNER_GRACE_SECONDS", 30.0, 2.0, 3600.0)
        self.lease_heartbeat_seconds = _env_float("AIN_FULL_TICK_LEASE_HEARTBEAT_SECONDS", 2.0, 0.5, 60.0); self.stats_heartbeat_seconds = _env_float("AIN_FULL_TICK_STATS_HEARTBEAT_SECONDS", 2.0, 0.5, 60.0)
        self.retention_mode = str(os.environ.get("LOG_RETENTION_MODE") or "MANUAL_ONLY").strip().upper()
        self.keep_policy = str(os.environ.get("LOG_KEEP_POLICY") or "KEEP_ALL").strip().upper()
        # R169.2 research default is non-destructive. Retention can only be executed
        # by an explicit manual maintenance command outside a live session.
        self.keep_wal = True
        self.wal_retention_days = 0
        self.parquet_enabled = _env_bool("AIN_FULL_TICK_PARQUET_ENABLED", True)
        self.compression = str(os.environ.get("AIN_FULL_TICK_PARQUET_COMPRESSION", "zstd") or "zstd").strip()
        self._uses_default_parquet_writer = parquet_writer is None
        self._parquet_writer = parquet_writer
        runtime = get_runtime()
        local_session = f"{os.getpid()}-{time.time_ns():x}"
        self.session_id = str(session_id or (runtime.session_id if runtime is not None else local_session))
        self.process_start_time = _process_start_time(os.getpid())
        self._queue: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(maxsize=self.queue_max)
        self._critical_queue: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(
            maxsize=max(256, min(4096, self.queue_max // 4))
        )
        self._convert_queue: "queue.Queue[Optional[Path]]" = queue.Queue()
        self._lock = threading.RLock()
        self._lease_lock = threading.RLock()
        self._segments: Dict[Tuple[str, str, str, str], _Segment] = {}
        self._overflow_handles: Dict[Tuple[str, str, str, str], _Segment] = {}
        self._overflow_partition_locks: Dict[Tuple[str, str, str, str], threading.RLock] = {}
        self._conversion_pending: set[str] = set()
        self._writer_thread: Optional[threading.Thread] = None
        self._converter_thread: Optional[threading.Thread] = None
        self._started = False
        self._accepting = True
        self._stop = threading.Event()
        # Wake the WAL writer immediately when either normal or critical work
        # arrives.  This avoids making decision-bearing durability wait behind
        # the old 100 ms normal-queue polling window while keeping idle CPU low.
        self._writer_wakeup = threading.Event()
        self._converter_stop = threading.Event()
        self._seq = 0
        self._segment_seq = 0
        self._seen_source_ids: "OrderedDict[str, None]" = OrderedDict()
        # Source ids are committed to dedup only after enqueue succeeds.  A
        # transient reservation must never masquerade as a recorded source fact.
        self._inflight_source_ids: set[str] = set()
        self._source_receipts_by_id: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._source_id_by_event_seq: Dict[int, str] = {}
        self._durability_failures: Dict[int, str] = {}
        # H12H14H3: an observation may first be captured as neutral research and
        # later, with the same immutable source_observation_id, become decision-
        # bearing before the periodic fsync.  Promote the *existing* WAL row to
        # immediate durability; never append a duplicate source row.
        self._force_durable_seqs: set[int] = set()
        self._durability_condition = threading.Condition(self._lock)
        self._seen_decision_ids: "OrderedDict[str, None]" = OrderedDict()
        self._seen_limit = _env_int("AIN_FULL_TICK_DEDUP_IDS", 500000, 10000, 5_000_000)
        self._symbol_source_seq: Dict[Tuple[str, str, str], int] = {}
        self._source_seq_by_id: "OrderedDict[str, int]" = OrderedDict()
        # Immutable publication/cross truth is owned by the canonical episode,
        # not by individual UI refresh rows.
        self._episode_immutables: Dict[str, Dict[str, Any]] = {}
        self._episode_last_decision_id: Dict[str, str] = {}
        self._durability_pending: "OrderedDict[int, float]" = OrderedDict()
        self._last_lease_write = 0.0; self._last_stats_write_monotonic = 0.0
        self._last_recovery_scan = 0.0
        self._stats: Dict[str, Any] = {
            "accepted_rows": 0,
            "queued": 0,
            "critical_queued": 0,
            "written_wal": 0,
            "fsynced_wal": 0,
            "last_enqueued_seq": 0,
            "last_wal_written_seq": 0,
            "last_fsynced_seq": 0,
            "source_observations": 0,
            "source_journal_durable_receipts": 0,
            "source_journal_durable_wait_timeouts": 0,
            "source_journal_durable_failures": 0,
            "source_journal_failed_receipt_retries": 0,
            "source_journal_enqueue_failures": 0,
            "source_journal_durable_promotions_requested": 0,
            "source_journal_durable_promotions_completed": 0,
            "source_journal_durable_promotions_failed": 0,
            "decision_enrichments": 0,
            "decision_duplicates": 0,
            "event_updates": 0,
            "decision_context_snapshots": 0,
            "decision_partial_context_failures": 0,
            "event_partial_context_failures": 0,
            "duplicates": 0,
            "spilled": 0,
            "dropped": 0,
            "errors_total": 0,
            "errors_by_stage": {
                "lease_heartbeat": 0, "recovery_scan": 0, "source_parse": 0,
                "cross_detection": 0, "feature_build": 0, "model_load": 0,
                "model_score": 0, "probability_attach": 0, "decision_write": 0,
                "context_write": 0, "event_write": 0, "wal_write": 0,
                "wal_fsync": 0, "parquet_conversion": 0, "provenance_write": 0,
                "radar_worker": 0, "ui_publish": 0,
            },
            "first_error": {},
            "last_error": {},
            "unclassified_errors": 0,
            "queue_peak": 0,
            "closed_segments": 0,
            "parquet_parts": 0,
            "parquet_rows": 0,
            "parquet_bytes": 0,
            "conversion_errors": 0,
            "parquet_deferred_no_engine": 0,
            "recovered_segments": 0,
            "active_open_skipped": 0,
            "unknown_open_deferred": 0,
            "retained_wal_deleted": 0,
            "shutdown_complete": False,
        }
        if start_immediately and self.enabled:
            self.start()

    def _record_error(
        self, stage: str, operation: str, exc: BaseException | str,
        *, row: Optional[Mapping[str, Any]] = None, reason_code: str = "INTERNAL_ERROR",
        recoverable: bool = True,
    ) -> None:
        payload = dict(row or {})
        stage_key = str(stage or "unclassified").strip().lower()
        now = _utc_now_iso()
        item = {
            "stage": stage_key.upper(), "operation": str(operation),
            "type": type(exc).__name__ if isinstance(exc, BaseException) else "ResearchStageError",
            "message": str(exc), "time": now,
            "symbol": str(payload.get("symbol") or ""),
            "episode_id": str(payload.get("episode_id") or payload.get("pulse_episode_id") or ""),
            "source_observation_id": str(payload.get("source_observation_id") or ""),
            "reason_code": str(reason_code or "INTERNAL_ERROR"),
        }
        with self._lock:
            self._stats["errors_total"] = int(self._stats.get("errors_total", 0)) + 1
            by_stage = self._stats.setdefault("errors_by_stage", {})
            if stage_key not in by_stage:
                by_stage[stage_key] = 0
                self._stats["unclassified_errors"] = int(self._stats.get("unclassified_errors", 0)) + 1
            by_stage[stage_key] = int(by_stage.get(stage_key, 0)) + 1
            if not self._stats.get("first_error"):
                self._stats["first_error"] = dict(item)
            self._stats["last_error"] = dict(item)
        record_stage_error(
            stage_key, operation, exc, market=str(payload.get("market_key") or ""),
            symbol=item["symbol"], episode_id=item["episode_id"],
            source_observation_id=item["source_observation_id"],
            trace_id=str(payload.get("trace_id") or ""), reason_code=reason_code,
            recoverable=recoverable,
        )
        # Errors are rare and diagnostically critical. Persist the current
        # counters immediately so an external validator never reads a stale
        # pre-error snapshot after a partial decision/context write.
        try:
            self._persist_stats_snapshot_atomic()
        except Exception as persist_exc:
            logging.getLogger("FullTickResearchRecorder").error(
                "تعذر تثبيت عدادات الخطأ فورًا: %s: %s",
                type(persist_exc).__name__, persist_exc, exc_info=True,
            )

    def is_enabled(self) -> bool:
        return bool(self.enabled)

    @property
    def lease_path(self) -> Path:
        return self.lock_root / f"recorder-{self.session_id}.lease.json"

    def _next_seq(self) -> int:
        with self._lock:
            self._seq += 1
            return self._seq

    def _next_segment_seq(self) -> int:
        with self._lock:
            self._segment_seq += 1
            return self._segment_seq

    def _partition_for(self, row: Mapping[str, Any]) -> Tuple[str, str, str, str]:
        dataset = _dataset_for_row(row)
        market = _slug(row.get("market_key"))
        session_date = _canonical_session_date_for_row(row, row.get("market_key"))
        hour = _hour_text(row.get("bar_datetime") or row.get("observed_at") or row.get("recorded_at_utc"))
        return dataset, market, session_date, hour

    def _partition_dir(self, root: Path, partition: Tuple[str, str, str, str]) -> Path:
        dataset, market, session_date, hour = partition
        path = root / f"dataset={dataset}" / f"market={market}" / f"session_date={session_date}" / f"hour={hour}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def _stats_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            snapshot = dict(self._stats)
            pending_times = list(self._durability_pending.values())
            now = time.time()
            snapshot.update({
                "version": VERSION,
                "schema_version": SCHEMA_VERSION,
                "recorder_session_id": self.session_id,
                "queue_size": int(self._queue.qsize()),
                "critical_queue_size": int(self._critical_queue.qsize()),
                "pending": int(getattr(self._queue, "unfinished_tasks", 0) or 0)
                + int(getattr(self._critical_queue, "unfinished_tasks", 0) or 0),
                "conversion_queue_size": int(self._convert_queue.qsize()),
                "durability_lag_rows": int(len(self._durability_pending)),
                "durable_promotion_pending": int(len(self._force_durable_seqs)),
                "durability_lag_ms": round(max(0.0, now - min(pending_times)) * 1000.0, 3) if pending_times else 0.0,
                "queue_lag_rows": max(0, int(self._stats["accepted_rows"]) - int(self._stats["written_wal"])),
                "wal_unfsynced_rows": max(0, int(self._stats["written_wal"]) - int(self._stats["fsynced_wal"])),
                "open_segments": len(self._segments),
                "open_overflow_segments": len(self._overflow_handles),
                "parquet_engine": parquet_engine_name(),
                "parquet_engine_available": bool(parquet_engine_available()),
                "parquet_enabled": bool(self.parquet_enabled),
                "keep_wal": True,
                "wal_retention_days": 0,
                "log_retention_mode": self.retention_mode,
                "log_keep_policy": self.keep_policy,
                "base_dir": str(self.base_dir),
                "captured_at_utc": _utc_now_iso(),
            })
            runtime_stats = runtime_error_stats()
            runtime_total = int(runtime_stats.get("errors_total", 0) or 0)
            runtime_by_stage = dict(runtime_stats.get("errors_by_stage") or {})
            runtime_by_reason = dict(runtime_stats.get("errors_by_reason_code") or {})
            runtime_freeze_blocking = int(runtime_stats.get("freeze_blocking_errors_total", runtime_total) or 0)
            runtime_nonblocking_recoverable = int(runtime_stats.get("freeze_nonblocking_recoverable_errors_total", 0) or 0)
            recorder_total = int(snapshot.get("errors_total", 0) or 0)
            recorder_by_stage = dict(snapshot.get("errors_by_stage") or {})
            # Recorder failures are forwarded to SessionRuntime, so summing the
            # two counters would double-count them. Merge stage-wise by max.
            combined_by_stage: Dict[str, int] = {}
            for stage in set(recorder_by_stage) | set(runtime_by_stage):
                combined_by_stage[str(stage)] = max(
                    int(recorder_by_stage.get(stage, 0) or 0),
                    int(runtime_by_stage.get(stage, 0) or 0),
                )
            snapshot["recorder_errors_total"] = recorder_total
            snapshot["recorder_errors_by_stage"] = recorder_by_stage
            snapshot["runtime_errors_total"] = runtime_total
            snapshot["runtime_errors_by_stage"] = runtime_by_stage
            snapshot["runtime_errors_by_reason_code"] = runtime_by_reason
            snapshot["runtime_freeze_blocking_errors_total"] = runtime_freeze_blocking
            snapshot["runtime_nonblocking_recoverable_errors_total"] = runtime_nonblocking_recoverable
            snapshot["freeze_blocking_errors_total"] = max(recorder_total, runtime_freeze_blocking)
            snapshot["errors_total"] = max(recorder_total, runtime_total, sum(combined_by_stage.values()))
            snapshot["errors_by_stage"] = combined_by_stage
            snapshot["runtime_first_error"] = dict(runtime_stats.get("first_error") or {})
            snapshot["runtime_last_error"] = dict(runtime_stats.get("last_error") or {})
            if not snapshot.get("first_error") and snapshot["runtime_first_error"]:
                snapshot["first_error"] = dict(snapshot["runtime_first_error"])
            if snapshot["runtime_last_error"]:
                snapshot["last_error"] = dict(snapshot["runtime_last_error"])
            return snapshot

    def stats(self) -> Dict[str, Any]:
        return self._stats_snapshot()

    def _persist_stats_snapshot_atomic(self) -> None:
        self.stats_root.mkdir(parents=True, exist_ok=True)
        day = dt.datetime.now().date().isoformat()
        snapshot = self._stats_snapshot()
        for path in (
            self.stats_root / f"full_tick_recorder_stats_{day}.json",
            self.stats_root / "latest_stats.json",
        ):
            write_json_atomic(
                path, snapshot, ensure_ascii=False, sort_keys=True, indent=2,
                trailing_newline=True,
                delays=DURABLE_STATE_REPLACE_DELAYS,
            )

    def _write_stats(self) -> None:
        try:
            self._persist_stats_snapshot_atomic(); self._last_stats_write_monotonic = time.monotonic()
        except Exception as exc:
            self._record_error("provenance_write", "write_recorder_stats", exc)

    def _write_stats_if_due(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if force or now - float(self._last_stats_write_monotonic or 0.0) >= self.stats_heartbeat_seconds:
            self._write_stats()

    def _write_lease(self, *, state: str = "active", force: bool = False) -> None:
        with self._lease_lock:
            now = time.time()
            if not force and now - self._last_lease_write < self.lease_heartbeat_seconds:
                return
            try:
                self.lock_root.mkdir(parents=True, exist_ok=True)
                payload = {
                    "version": VERSION,
                    "recorder_session_id": self.session_id,
                    "pid": os.getpid(),
                    "process_start_time": self.process_start_time,
                    "state": state,
                    "heartbeat_epoch_s": now,
                    "heartbeat_at_utc": _utc_now_iso(),
                }
                # R169.3.5: use the same bounded Windows sharing-lock contract as
                # worker IPC.  The prior 120ms retry window was shorter than the
                # Defender/indexer holds observed in the full live session.
                write_json_atomic(
                    self.lease_path, payload, ensure_ascii=False, sort_keys=True,
                    indent=2, trailing_newline=True,
                )
                self._last_lease_write = now
            except Exception as exc:
                self._record_error(
                    "lease_heartbeat", "write_lease", exc,
                    reason_code="LEASE_WRITE_FAILED_AFTER_RETRY_BUDGET",
                )

    def _remove_lease(self) -> None:
        try:
            self.lease_path.unlink(missing_ok=True)
        except Exception as exc:
            self._record_error("lease_heartbeat", "remove_lease", exc, reason_code="LEASE_REMOVE_FAILED")

    def _owner_session_from_open(self, path: Path) -> str:
        """Recover the exact owner session from both legacy and R169.2 names.

        Session IDs may contain several dashes (timestamp, PID and random suffix),
        therefore a permissive regex is unsafe: it can truncate the identity and
        make an active writer look orphaned.  The filename contract is parsed by
        its fixed prefix/suffix instead.
        """
        name = str(path.name or "")
        if name.startswith("segment-") and name.endswith(".jsonl.open"):
            body = name[len("segment-"):-len(".jsonl.open")]
            head, sep, tail = body.rpartition("-")
            if sep and tail.isdigit() and len(tail) == 6:
                return head
            return body
        if name.startswith("overflow-") and name.endswith(".jsonl.open"):
            return name[len("overflow-"):-len(".jsonl.open")]
        return ""

    def _owner_state(self, owner_session: str) -> str:
        if not owner_session:
            return "unknown"
        lease_path = self.lock_root / f"recorder-{owner_session}.lease.json"
        expected_start: Optional[float] = None
        pid: Optional[int] = None
        if lease_path.exists():
            try:
                payload = json.loads(lease_path.read_text(encoding="utf-8"))
                expected_start = payload.get("process_start_time")
                raw_pid = payload.get("pid")
                pid = int(raw_pid) if raw_pid is not None else None
            except Exception as exc:
                self._record_error(
                    "recovery_scan", "read_owner_lease", exc,
                    reason_code="OWNER_LEASE_READ_FAILED",
                )
        if pid is None:
            # Backward compatibility for the legacy ``<pid>-<suffix>`` owner ID.
            first = owner_session.split("-", 1)[0]
            if first.isdigit():
                pid = int(first)
            else:
                # R169.2 IDs contain ``...Z-<pid>-<suffix>``.
                parts = owner_session.split("-")
                for token in reversed(parts[:-1]):
                    if token.isdigit():
                        pid = int(token)
                        break
        if pid is None:
            return "unknown"
        alive = _process_matches(pid, expected_start)
        if alive is True:
            return "active"
        if alive is False:
            return "dead"
        return "unknown"

    def _enqueue_conversion(self, path: Path) -> None:
        key = str(path.resolve())
        with self._lock:
            if key in self._conversion_pending:
                return
            self._conversion_pending.add(key)
        self._convert_queue.put(path)

    def _recover_orphan_segments(self, *, unknown_grace_seconds: Optional[float] = None) -> Dict[str, int]:
        result = {"recovered": 0, "active_skipped": 0, "unknown_deferred": 0}
        if not self.wal_root.exists():
            return result
        grace = self.unknown_owner_grace_seconds if unknown_grace_seconds is None else max(0.0, float(unknown_grace_seconds))
        now = time.time()
        for path in list(self.wal_root.rglob("*.jsonl.open")):
            try:
                owner_session = self._owner_session_from_open(path)
                if owner_session == self.session_id:
                    continue
                state = self._owner_state(owner_session)
                if state == "active":
                    result["active_skipped"] += 1
                    continue
                age = max(0.0, now - path.stat().st_mtime)
                if state == "unknown" and age < grace:
                    result["unknown_deferred"] += 1
                    continue
                closed = path.with_suffix(".closed")
                os.replace(path, closed)
                result["recovered"] += 1
                self._enqueue_conversion(closed)
                if owner_session:
                    try:
                        (self.lock_root / f"recorder-{owner_session}.lease.json").unlink(missing_ok=True)
                    except Exception as exc:
                        self._record_error(
                            "recovery_scan", "remove_recovered_owner_lease", exc,
                            row={"source_file": str(path), "owner_session": owner_session},
                            reason_code="RECOVERED_OWNER_LEASE_REMOVE_FAILED",
                        )
            except Exception as exc:
                self._record_error("recovery_scan", "recover_orphan_segment", exc, row={"source_file": str(path)}, reason_code="RECOVERY_SCAN_FAILED")
        with self._lock:
            self._stats["recovered_segments"] += result["recovered"]
            self._stats["active_open_skipped"] += result["active_skipped"]
            self._stats["unknown_open_deferred"] += result["unknown_deferred"]
        for path in self.wal_root.rglob("*.jsonl.closed"):
            if not self._conversion_complete(path):
                self._enqueue_conversion(path)
        return result

    def start(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            if self._started:
                return
            self.base_dir.mkdir(parents=True, exist_ok=True)
            self.wal_root.mkdir(parents=True, exist_ok=True)
            self.parquet_root.mkdir(parents=True, exist_ok=True)
            self.stats_root.mkdir(parents=True, exist_ok=True)
            self.lock_root.mkdir(parents=True, exist_ok=True)
            self._stop.clear()
            self._converter_stop.clear()
            self._write_lease(force=True)
            self._last_recovery_scan = time.time()
            self._writer_thread = threading.Thread(target=self._writer_loop, name="AinFullTickWalWriter", daemon=True)
            self._converter_thread = threading.Thread(target=self._converter_loop, name="AinFullTickParquetConverter", daemon=True)
            self._writer_thread.start()
            self._converter_thread.start()
            self._started = True
        self._recover_orphan_segments()
        self._cleanup_retention()
        self._write_stats()

    def _remember_source_id_locked(self, source_id: str) -> bool:
        if source_id in self._seen_source_ids:
            self._seen_source_ids.move_to_end(source_id)
            self._stats["duplicates"] += 1
            return False
        self._seen_source_ids[source_id] = None
        if len(self._seen_source_ids) > self._seen_limit:
            for _ in range(max(1, self._seen_limit // 20)):
                try:
                    self._seen_source_ids.popitem(last=False)
                except KeyError:
                    break
        return True

    def _source_receipt_locked(self, source_id: str) -> Dict[str, Any]:
        receipt = dict(self._source_receipts_by_id.get(str(source_id or "")) or {})
        if receipt:
            seq = int(receipt.get("event_seq") or 0)
            failed = bool(seq in self._durability_failures or receipt.get("failed"))
            receipt["wal_written"] = bool(receipt.get("wal_written") or (seq > 0 and seq <= int(self._stats.get("last_wal_written_seq", 0))))
            receipt["durable"] = bool(
                (not failed) and (
                    receipt.get("durable")
                    or (seq > 0 and seq <= int(self._stats.get("last_fsynced_seq", 0)) and seq not in self._durability_pending)
                )
            )
            if failed:
                receipt["failed"] = True
                receipt["durable"] = False
                receipt["reason"] = str(self._durability_failures.get(seq) or receipt.get("reason") or "WAL_WRITE_FAILED")
        return receipt

    def _remember_source_receipt_locked(self, source_id: str, receipt: Mapping[str, Any]) -> None:
        key = str(source_id or "").strip()
        if not key:
            return
        self._source_receipts_by_id[key] = dict(receipt or {})
        self._source_receipts_by_id.move_to_end(key)
        while len(self._source_receipts_by_id) > self._seen_limit:
            old_id, old_receipt = self._source_receipts_by_id.popitem(last=False)
            seq = int((old_receipt or {}).get("event_seq") or 0)
            if seq:
                self._source_id_by_event_seq.pop(seq, None)

    def _request_durable_promotion(self, event_seq: int) -> bool:
        """Force fsync of an already accepted source row without duplicating it."""
        seq = int(event_seq or 0)
        if seq <= 0:
            return False
        with self._durability_condition:
            receipt_source = self._source_id_by_event_seq.get(seq)
            if seq in self._durability_failures:
                return False
            if seq not in self._durability_pending:
                # Already durable (or no longer a live receipt).
                return bool(receipt_source)
            if seq not in self._force_durable_seqs:
                self._force_durable_seqs.add(seq)
                self._stats["source_journal_durable_promotions_requested"] = int(
                    self._stats.get("source_journal_durable_promotions_requested", 0)
                ) + 1
            self._writer_wakeup.set()
            self._durability_condition.notify_all()
            return True

    def _service_force_durable(self) -> None:
        """Writer-thread service for rows written before they became critical."""
        with self._durability_condition:
            requested = set(int(x) for x in self._force_durable_seqs if int(x) > 0)
        if not requested:
            return
        # A row already written but awaiting the periodic fsync lives in exactly
        # one open segment.  Syncing that segment makes all of its preceding rows
        # durable too, which is conservative and keeps ordering intact.
        for segment in list(self._segments.values()):
            if requested.intersection(int(x) for x in segment.unfsynced_seqs):
                self._sync_segment(segment)

    def _wait_for_source_durable(self, source_id: str, event_seq: int, timeout: Optional[float] = None) -> Dict[str, Any]:
        wait_timeout = float(timeout if timeout is not None else _env_float(
            "AIN_SOURCE_JOURNAL_DURABLE_RECEIPT_TIMEOUT_SECONDS", 2.0, 0.10, 30.0
        ))
        deadline = time.monotonic() + wait_timeout
        with self._durability_condition:
            while True:
                receipt = self._source_receipt_locked(source_id)
                if receipt.get("durable") or receipt.get("failed"):
                    return receipt
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._stats["source_journal_durable_wait_timeouts"] = int(
                        self._stats.get("source_journal_durable_wait_timeouts", 0)
                    ) + 1
                    receipt["durable_wait_timeout"] = True
                    receipt.setdefault("reason", "SOURCE_JOURNAL_DURABILITY_TIMEOUT")
                    return receipt
                self._durability_condition.wait(timeout=remaining)

    def _mark_durability_failed(self, seq: int, reason: str) -> None:
        seq = int(seq or 0)
        if seq <= 0:
            return
        with self._durability_condition:
            self._durability_pending.pop(seq, None)
            if seq in self._force_durable_seqs:
                self._force_durable_seqs.discard(seq)
                self._stats["source_journal_durable_promotions_failed"] = int(
                    self._stats.get("source_journal_durable_promotions_failed", 0)
                ) + 1
            self._durability_failures[seq] = str(reason or "WAL_WRITE_FAILED")
            self._stats["source_journal_durable_failures"] = int(
                self._stats.get("source_journal_durable_failures", 0)
            ) + 1
            source_id = self._source_id_by_event_seq.get(seq)
            if source_id:
                receipt = self._source_receipt_locked(source_id)
                receipt.update({"failed": True, "durable": False, "reason": self._durability_failures[seq]})
                self._remember_source_receipt_locked(source_id, receipt)
            self._durability_condition.notify_all()

    def _reserve_decision_id_locked(self, decision_id: str) -> bool:
        """Reserve a decision identity so exact retries remain idempotent."""
        key = str(decision_id or "").strip()
        if not key:
            return True
        if key in self._seen_decision_ids:
            self._seen_decision_ids.move_to_end(key)
            self._stats["decision_duplicates"] = int(
                self._stats.get("decision_duplicates", 0)
            ) + 1
            return False
        self._seen_decision_ids[key] = None
        if len(self._seen_decision_ids) > self._seen_limit:
            for _ in range(max(1, self._seen_limit // 20)):
                try:
                    self._seen_decision_ids.popitem(last=False)
                except KeyError:
                    break
        return True

    def _prepare_row(self, payload: Mapping[str, Any]) -> Dict[str, Any]:
        row = _normalize_mapping(payload, excluded={"_tail_records"})
        row.setdefault("schema_version", SCHEMA_VERSION)
        row.setdefault("recorder_version", VERSION)
        # Recorder ownership is local process truth, never caller-provided metadata.
        # A stale/replayed payload must not be able to claim a previous recorder
        # session and contaminate a frozen sample cohort.
        row["recorder_session_id"] = self.session_id
        row.setdefault("session_id", self.session_id)
        row.setdefault("event_seq", self._next_seq())
        row.setdefault("recorded_at_utc", _utc_now_iso())
        row["session_date"] = _canonical_session_date_for_row(row, row.get("market_key") or row.get("canonical_market_key"))
        row.setdefault("dataset_name", _dataset_for_row(row))
        row.setdefault("detector_family", DETECTOR_FAMILY)
        row.setdefault("timeframe", TIMEFRAME)
        if row.get("dataset_name") in {"decision_enrichments", "event_updates", "decision_context_snapshots"}:
            row = _truth_apply_trader_columns(row, for_ui=False)
        market = canonical_runtime_key(row.get("market_key") or row.get("canonical_market_key"))
        row["canonical_market_key"] = market
        row["market_key"] = market
        symbol = str(row.get("symbol") or "").strip().upper()
        signal_bar = (
            row.get("episode_signal_bar_time") or row.get("signal_bar_time")
            or row.get("probability_bar_time") or row.get("bar_datetime")
        )
        identity_bar = signal_bar
        if row.get("dataset_name") == "source_observations" and signal_bar:
            normalized_bar = _source_bar_to_market_naive(
                signal_bar, market_key=market,
                observed_at=row.get("observed_at") or row.get("source_observed_at"),
            )
            if normalized_bar is not None:
                identity_bar = normalized_bar.strftime("%Y-%m-%d %H:%M:%S")
        if symbol and identity_bar:
            identity = episode_identity(market, symbol, identity_bar)
            for key, value in identity.items():
                row.setdefault(key, value)
            row.setdefault("trace_id", trace_identity(row, market_key=market, symbol=symbol, signal_bar_time=identity_bar))
        row.setdefault("extra_json", {})
        return row

    def _mark_accepted(self, row: Mapping[str, Any]) -> None:
        seq = int(row.get("event_seq") or 0)
        with self._lock:
            self._stats["accepted_rows"] += 1
            self._stats["last_enqueued_seq"] = max(int(self._stats["last_enqueued_seq"]), seq)
            self._durability_pending[seq] = time.time()

    def _unmark_failed_accept(self, row: Mapping[str, Any]) -> None:
        seq = int(row.get("event_seq") or 0)
        with self._lock:
            self._durability_pending.pop(seq, None)
            self._stats["accepted_rows"] = max(0, int(self._stats["accepted_rows"]) - 1)

    def _mark_written(self, seq: int) -> None:
        seq = int(seq or 0)
        with self._durability_condition:
            self._stats["written_wal"] += 1
            self._stats["last_wal_written_seq"] = max(int(self._stats["last_wal_written_seq"]), seq)
            source_id = self._source_id_by_event_seq.get(seq)
            if source_id:
                receipt = self._source_receipt_locked(source_id)
                receipt["wal_written"] = True
                self._remember_source_receipt_locked(source_id, receipt)
            self._durability_condition.notify_all()

    def _mark_fsynced(self, seqs: Sequence[int]) -> None:
        if not seqs:
            return
        with self._durability_condition:
            unique = list(dict.fromkeys(int(x) for x in seqs if int(x) > 0))
            newly_durable = 0
            for seq in unique:
                if seq in self._durability_pending:
                    self._durability_pending.pop(seq, None)
                    newly_durable += 1
                if seq in self._force_durable_seqs:
                    self._force_durable_seqs.discard(seq)
                    self._stats["source_journal_durable_promotions_completed"] = int(
                        self._stats.get("source_journal_durable_promotions_completed", 0)
                    ) + 1
                self._durability_failures.pop(seq, None)
                source_id = self._source_id_by_event_seq.get(seq)
                if source_id:
                    receipt = self._source_receipt_locked(source_id)
                    was_durable = bool(receipt.get("durable"))
                    receipt.update({"wal_written": True, "durable": True, "failed": False, "reason": "WAL_FSYNCED"})
                    self._remember_source_receipt_locked(source_id, receipt)
                    if not was_durable:
                        self._stats["source_journal_durable_receipts"] = int(
                            self._stats.get("source_journal_durable_receipts", 0)
                        ) + 1
            self._stats["fsynced_wal"] += newly_durable
            if unique:
                self._stats["last_fsynced_seq"] = max(int(self._stats["last_fsynced_seq"]), max(unique))
            self._durability_condition.notify_all()

    def _overflow_lock_for(self, partition: Tuple[str, str, str, str]) -> threading.RLock:
        with self._lock:
            return self._overflow_partition_locks.setdefault(partition, threading.RLock())

    def _spill_sync(self, row: Dict[str, Any]) -> bool:
        """Rare queue-saturation path. Uses a per-partition lock and fsyncs immediately."""
        partition = self._partition_for(row)
        lock = self._overflow_lock_for(partition)
        try:
            with lock:
                segment = self._overflow_handles.get(partition)
                if segment is None:
                    directory = self._partition_dir(self.wal_root, partition)
                    path = directory / f"overflow-{self.session_id}.jsonl.open"
                    handle = path.open("a", encoding="utf-8", buffering=1)
                    now = time.time()
                    segment = _Segment(partition, path, handle, 0, 0, now, now, now, 0)
                    self._overflow_handles[partition] = segment
                line = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
                segment.handle.write(line)
                segment.handle.flush()
                os.fsync(segment.handle.fileno())
                segment.rows += 1
                segment.bytes_written += len(line.encode("utf-8"))
                seq = int(row.get("event_seq") or 0)
                self._mark_written(seq)
                self._mark_fsynced([seq])
            with self._lock:
                self._stats["spilled"] += 1
            return True
        except Exception as exc:
            self._unmark_failed_accept(row)
            with self._lock:
                self._stats["dropped"] += 1
            self._record_error("wal_write", "spill_sync", exc, row=row, reason_code="WAL_SPILL_FAILED")
            return False

    def _enqueue_prepared(self, row: Dict[str, Any]) -> bool:
        if not self.enabled or not self._accepting:
            return False
        self.start()
        self._mark_accepted(row)
        target = self._critical_queue if self._critical_durability_row(row) else self._queue
        try:
            target.put_nowait(row)
            self._writer_wakeup.set()
            with self._lock:
                if target is self._critical_queue:
                    self._stats["critical_queued"] += 1
                else:
                    self._stats["queued"] += 1
                total_q = int(self._queue.qsize()) + int(self._critical_queue.qsize())
                self._stats["queue_peak"] = max(int(self._stats["queue_peak"]), total_q)
            return True
        except queue.Full:
            return self._spill_sync(row)

    def enqueue(self, payload: Mapping[str, Any], *, dedup_source: bool = False) -> bool:
        if not self.enabled or not self._accepting:
            return False
        row = self._prepare_row(payload)
        if dedup_source:
            source_id = str(row.get("source_observation_id") or "")
            with self._lock:
                if not self._remember_source_id_locked(source_id):
                    return False
        return self._enqueue_prepared(row)

    def record_source_observation_receipt(
        self, *, market_key: str, observation: Mapping[str, Any],
        runtime_context: Optional[Mapping[str, Any]] = None,
        duplicate_is_success: bool = False,
        require_durable: Optional[bool] = None,
        durable_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not self.enabled or not self._accepting:
            return {"accepted": False, "enqueued": False, "durable": False, "reason": "RECORDER_DISABLED"}
        self.start()
        obs = dict(observation or {})
        runtime = dict(runtime_context or {})
        symbol = str(obs.get("symbol") or "").strip().upper()
        observation_id = str(obs.get("source_observation_id") or "").strip()
        market = canonical_runtime_key(market_key or obs.get("market_key"))
        session_date = _canonical_session_date_for_row(obs, market)
        durability_required = bool(
            require_durable if require_durable is not None
            else (
                obs.get("positive_cross_now") or obs.get("negative_cross_now")
                or runtime.get("decision_bearing_source")
                or runtime.get("source_journal_durability_required")
            )
        )
        if not symbol or not observation_id:
            self._record_error(
                "source_parse", "record_source_observation",
                "SOURCE_ID_OR_SYMBOL_MISSING", row={**obs, "market_key": market},
                reason_code="SOURCE_ID_OR_SYMBOL_MISSING",
            )
            return {"accepted": False, "enqueued": False, "durable": False, "reason": "SOURCE_ID_OR_SYMBOL_MISSING"}

        with self._lock:
            existing = self._source_receipt_locked(observation_id)
            committed_seen = observation_id in self._seen_source_ids
            # A writer-level durability failure is not a committed duplicate.
            # Release the failed receipt so the exact immutable source fact can
            # be re-enqueued after storage recovers. A partially written old WAL
            # row is harmless because source_observation_id remains the semantic
            # dedup identity for downstream replay.
            if committed_seen and existing and bool(existing.get("failed")):
                failed_seq = int(existing.get("event_seq") or 0)
                self._seen_source_ids.pop(observation_id, None)
                self._source_receipts_by_id.pop(observation_id, None)
                if failed_seq:
                    self._source_id_by_event_seq.pop(failed_seq, None)
                self._stats["source_journal_failed_receipt_retries"] = int(
                    self._stats.get("source_journal_failed_receipt_retries", 0)
                ) + 1
                committed_seen = False
                existing = {}
            elif committed_seen and not existing:
                # Never turn a bounded/legacy seen marker into a success receipt
                # when the receipt evidence itself is unavailable. Recreate the
                # exact row rather than returning an unprovable duplicate ACK.
                self._seen_source_ids.pop(observation_id, None)
                committed_seen = False

            if committed_seen and existing:
                self._seen_source_ids.move_to_end(observation_id)
                self._stats["duplicates"] += 1
                duplicate = dict(existing)
                duplicate["duplicate"] = True
                duplicate["accepted"] = bool(duplicate.get("enqueued"))
            elif observation_id in self._inflight_source_ids:
                return {
                    "accepted": False, "enqueued": False, "durable": False,
                    "inflight": True, "duplicate": True, "reason": "SOURCE_JOURNAL_INFLIGHT",
                    "source_observation_id": observation_id,
                }
            else:
                duplicate = {}
                self._inflight_source_ids.add(observation_id)

        if duplicate:
            if durability_required and not duplicate.get("durable") and not duplicate.get("failed"):
                self._request_durable_promotion(int(duplicate.get("event_seq") or 0))
                duplicate = self._wait_for_source_durable(
                    observation_id, int(duplicate.get("event_seq") or 0), timeout=durable_timeout
                )
                duplicate["duplicate"] = True
                duplicate["accepted"] = bool(duplicate.get("enqueued"))
            duplicate["receipt_ok"] = bool(
                duplicate.get("durable") if durability_required else duplicate.get("enqueued")
            )
            duplicate["duplicate_is_success"] = bool(duplicate_is_success)
            return duplicate

        with self._lock:
            seq_key = (market, session_date, symbol)
            symbol_source_seq = int(self._symbol_source_seq.get(seq_key, 0) or 0) + 1
            self._symbol_source_seq[seq_key] = symbol_source_seq
        signal_bar = obs.get("signal_bar_time") or obs.get("bar_datetime")
        source_latency_ms = _elapsed_ms(obs.get("source_detected_at"))
        row: Dict[str, Any] = {
            "record_kind": "SOURCE_OBSERVATION", "dataset_name": "source_observations",
            "event_type": "SOURCE_OBSERVATION", "market_key": market,
            "market_family": _canonical_market_family(market), "session_date": session_date,
            "canonical_market_session_date": session_date, "market_session_date": session_date,
            "symbol": symbol, "name": obs.get("name"),
            "source_observation_id": observation_id,
            "source_observation_seq": obs.get("source_observation_seq"),
            "symbol_source_seq": symbol_source_seq, "symbol_source_seq_scope": "RECORDER_SESSION",
            "source_file": obs.get("source_file"), "source_mtime": obs.get("source_mtime"),
            "source_mtime_ns": obs.get("source_mtime_ns"), "source_size": obs.get("source_size"),
            "source_tail_sha256": obs.get("source_tail_sha256"),
            "source_snapshot_id": obs.get("source_snapshot_id"),
            "source_reconciliation_kind": obs.get("source_reconciliation_kind"),
            "exact_source_bar_materialized": obs.get("exact_source_bar_materialized"),
            "exact_source_bar_record_status": obs.get("exact_source_bar_record_status"),
            "seal_trigger_source_observation_id": obs.get("seal_trigger_source_observation_id"),
            "seal_trigger_bar_time": obs.get("seal_trigger_bar_time"),
            "source_snapshot_before_size": obs.get("source_snapshot_before_size"),
            "source_snapshot_before_mtime_ns": obs.get("source_snapshot_before_mtime_ns"),
            "source_snapshot_before_tail_sha256": obs.get("source_snapshot_before_tail_sha256"),
            "source_snapshot_after_size": obs.get("source_snapshot_after_size"),
            "source_snapshot_after_mtime_ns": obs.get("source_snapshot_after_mtime_ns"),
            "source_snapshot_after_tail_sha256": obs.get("source_snapshot_after_tail_sha256"),
            "regular_session_bar_close_1500": obs.get("regular_session_bar_close_1500"),
            "regular_session_bar_time_1500": obs.get("regular_session_bar_time_1500"),
            "official_session_close_1530": obs.get("official_session_close_1530"),
            "official_close_bar_time": obs.get("official_close_bar_time"),
            "official_close_finality_state": obs.get("official_close_finality_state"),
            "regular_session_close_reason_code": obs.get("regular_session_close_reason_code"),
            "official_session_close_reason_code": obs.get("official_session_close_reason_code"),
            "official_close_used_for_model_signal": obs.get("official_close_used_for_model_signal"),
            "official_close_used_for_outcomes": obs.get("official_close_used_for_outcomes"),
            "source_detected_at": obs.get("source_detected_at"), "engine_received_at_utc": _utc_now_iso(),
            "observed_at": obs.get("observed_at"), "bar_datetime": obs.get("bar_datetime"),
            "bar_date": obs.get("bar_date"), "source_lane": obs.get("live_sniper_source_lane"),
            "source_freshness_reason_code": obs.get("source_freshness_reason_code"),
            "source_market_date": obs.get("source_market_date"),
            "source_bar_date": obs.get("source_bar_date"),
            "source_content_stale": bool(obs.get("_source_content_stale")),
            "signal_bar_time": signal_bar, "current_bar_time": obs.get("bar_datetime"),
            "current_price": obs.get("close"), "detector_family": "GANN20", "timeframe": "30M",
            "source_latency_ms": source_latency_ms,
            "open": obs.get("open"), "high": obs.get("high"), "low": obs.get("low"),
            "close": obs.get("close"), "volume": obs.get("volume"),
            "rsiscaled": obs.get("rsiscaled"), "var3": obs.get("var3"),
            "pulse_gap": obs.get("pulse_gap"), "pulse_gap_pct": obs.get("pulse_gap_pct"),
            "previous_gap": obs.get("previous_gap"), "previous_gap_pct": obs.get("previous_gap_pct"),
            "positive_cross_now": obs.get("positive_cross_now"), "negative_cross_now": obs.get("negative_cross_now"),
            "positive_pulse_age_bars": obs.get("positive_pulse_age_bars"),
            "negative_pulse_age_bars": obs.get("negative_pulse_age_bars"),
            "last_positive_cross_bar": obs.get("last_positive_cross_bar"),
            "last_positive_cross_price": obs.get("last_positive_cross_price"),
            "pavwap": obs.get("pavwap"), "anchor_low": obs.get("anchor_low"),
            "highest_since_pulse": obs.get("highest_since_pulse"),
            "retention_50": obs.get("retention_50"), "defense_38": obs.get("defense_38"),
            "atr14": obs.get("atr14"), "ret8": obs.get("ret8"),
            "prior_high40": obs.get("prior_high40"), "air_room40": obs.get("air_room40"),
            "close_position": obs.get("close_position"), "upper_wick_pct": obs.get("upper_wick_pct"),
            "volume_ratio20": obs.get("volume_ratio20"), "bar_extension_atr": obs.get("bar_extension_atr"),
            "tail_rows": obs.get("tail_rows"), "session_bar_times_json": obs.get("session_bar_times"),
            "source_journal_durability_required": durability_required,
            "extra_json": {"runtime": runtime},
        }
        prepared = self._prepare_row(row)
        event_seq = int(prepared.get("event_seq") or 0)
        accepted = self._enqueue_prepared(prepared)
        if not accepted:
            with self._lock:
                self._inflight_source_ids.discard(observation_id)
                self._stats["source_journal_enqueue_failures"] = int(
                    self._stats.get("source_journal_enqueue_failures", 0)
                ) + 1
            return {
                "accepted": False, "enqueued": False, "durable": False,
                "event_seq": event_seq, "source_observation_id": observation_id,
                "reason": "SOURCE_JOURNAL_ENQUEUE_FAILED",
            }

        receipt = {
            "accepted": True, "enqueued": True, "wal_written": False, "durable": False,
            "failed": False, "event_seq": event_seq, "source_observation_id": observation_id,
            "durability_required": durability_required, "reason": "SOURCE_JOURNAL_ENQUEUED",
        }
        with self._lock:
            self._inflight_source_ids.discard(observation_id)
            self._seen_source_ids[observation_id] = None
            self._seen_source_ids.move_to_end(observation_id)
            while len(self._seen_source_ids) > self._seen_limit:
                try:
                    self._seen_source_ids.popitem(last=False)
                except KeyError:
                    break
            self._source_seq_by_id[observation_id] = symbol_source_seq
            self._source_seq_by_id.move_to_end(observation_id)
            if len(self._source_seq_by_id) > self._seen_limit:
                self._source_seq_by_id.popitem(last=False)
            self._source_id_by_event_seq[event_seq] = observation_id
            self._remember_source_receipt_locked(observation_id, receipt)
            self._stats["source_observations"] += 1

        if durability_required:
            receipt = self._wait_for_source_durable(observation_id, event_seq, timeout=durable_timeout)
        else:
            with self._lock:
                receipt = self._source_receipt_locked(observation_id)
        receipt["receipt_ok"] = bool(receipt.get("durable") if durability_required else receipt.get("enqueued"))
        return receipt

    def record_source_observation(
        self, *, market_key: str, observation: Mapping[str, Any],
        runtime_context: Optional[Mapping[str, Any]] = None,
        duplicate_is_success: bool = False,
        require_durable: Optional[bool] = None,
        durable_timeout: Optional[float] = None,
    ) -> bool:
        receipt = self.record_source_observation_receipt(
            market_key=market_key, observation=observation,
            runtime_context=runtime_context, duplicate_is_success=duplicate_is_success,
            require_durable=require_durable, durable_timeout=durable_timeout,
        )
        if receipt.get("duplicate") and not duplicate_is_success:
            return False
        return bool(receipt.get("receipt_ok"))

    def record_decision_enrichment(
        self, *, market_key: str, observation: Mapping[str, Any],
        probability: Optional[Mapping[str, Any]] = None,
        paac_row: Optional[Mapping[str, Any]] = None,
        live_row: Optional[Mapping[str, Any]] = None,
        runtime_context: Optional[Mapping[str, Any]] = None,
        event_type: str = "DECISION_ENRICHMENT",
        write_context: bool = True,
    ) -> bool:
        _decision_write_t0 = time.perf_counter()
        obs, prob, paac, live = map(dict, (observation or {}, probability or {}, paac_row or {}, live_row or {}))
        runtime = dict(runtime_context or {})
        symbol = str(obs.get("symbol") or live.get("symbol") or paac.get("symbol") or "").strip().upper()
        source_id = str(obs.get("source_observation_id") or prob.get("probability_source_observation_id") or live.get("source_observation_id") or "")
        market = canonical_runtime_key(market_key or obs.get("market_key") or live.get("market_key"))
        signal_bar = obs.get("episode_signal_bar_time") or live.get("episode_signal_bar_time") or live.get("signal_bar_time") or prob.get("signal_bar_time") or prob.get("probability_bar_time") or obs.get("bar_datetime")
        if not symbol or not source_id or not signal_bar:
            self._record_error(
                "decision_write", "record_decision_enrichment", "DECISION_LINK_FIELDS_MISSING",
                row={**obs, **live, "market_key": market}, reason_code="DECISION_LINK_FIELDS_MISSING",
            )
            return False
        identity = episode_identity(market, symbol, signal_bar)
        trace_id = str(prob.get("trace_id") or trace_identity(obs, market_key=market, symbol=symbol, signal_bar_time=signal_bar))
        completed = prob.get("probability_completed_time") or runtime.get("decision_completed_time") or _utc_now_iso()
        attempted = prob.get("probability_attempt_time") or runtime.get("decision_attempt_time") or obs.get("observed_at") or completed
        probability_available = bool(prob.get("probability_available", prob.get("available")))
        rejects = rejection_fields(prob, live)
        record_id = str(prob.get("record_id") or live.get("decision_enrichment_id") or f"DEC-{identity['episode_key_sha256'][:24]}-{hashlib.sha256((source_id+str(attempted)).encode()).hexdigest()[:10]}")
        p50 = prob.get("p50", prob.get("p50_pct", prob.get("p50_live")))
        p100 = prob.get("p100", prob.get("p100_pct", prob.get("p100_live")))
        current_price = (
            live.get("current_price")
            if live.get("current_price") not in (None, "")
            else obs.get("source_latest_close")
            if obs.get("source_latest_close") not in (None, "")
            else obs.get("close")
        )
        proposed_first_cross_time = (
            live.get("first_cross_time") or paac.get("first_cross_at")
            or obs.get("last_positive_cross_bar") or signal_bar
        )
        proposed_first_cross_price = live.get("first_cross_price") or paac.get("first_cross_price")
        if proposed_first_cross_price in (None, ""):
            _obs_cross_time = obs.get("last_positive_cross_bar")
            if _truth_wall_time_key(_obs_cross_time) == _truth_wall_time_key(proposed_first_cross_time):
                proposed_first_cross_price = obs.get("last_positive_cross_price")
        proposed_appearance_price = live.get("appearance_price") or live.get("signal_entry_price")
        proposed_appearance_time = (
            live.get("appearance_time") or live.get("appearance_at")
            or live.get("signal_published_at") or live.get("ui_published_at")
        )
        with self._lock:
            immutable = self._episode_immutables.setdefault(identity["episode_id"], {})
            if not immutable.get("first_cross_time") and proposed_first_cross_time not in (None, ""):
                immutable["first_cross_time"] = proposed_first_cross_time
            if immutable.get("first_cross_price") in (None, "") and proposed_first_cross_price not in (None, ""):
                immutable["first_cross_price"] = proposed_first_cross_price
            if immutable.get("appearance_price") in (None, "") and proposed_appearance_price not in (None, ""):
                immutable["appearance_price"] = proposed_appearance_price
            if not immutable.get("appearance_time") and proposed_appearance_time not in (None, ""):
                immutable["appearance_time"] = proposed_appearance_time
            first_cross_time = immutable.get("first_cross_time") or proposed_first_cross_time
            first_cross_price = immutable.get("first_cross_price")
            appearance_price = immutable.get("appearance_price")
            appearance_time = immutable.get("appearance_time")
        probability_anchor = (
            prob.get("anchor_price")
            if prob.get("anchor_price") not in (None, "")
            else live.get("probability_anchor_price")
            if live.get("probability_anchor_price") not in (None, "")
            else live.get("gann20_anchor")
        )
        _sealed_truth_input = {**obs, **live, "signal_bar_time": signal_bar}
        sealed_close, sealed_close_reason = _truth_verified_sealed_close(_sealed_truth_input, signal_bar_time=signal_bar)
        sealed_source_observation_id = str(
            live.get("sealed_close_source_observation_id")
            or obs.get("sealed_close_source_observation_id")
            or ""
        ).strip()
        if (
            not sealed_source_observation_id
            and sealed_close is not None
            and _truth_wall_time_key(obs.get("bar_datetime")) == _truth_wall_time_key(signal_bar)
        ):
            try:
                _obs_close = float(obs.get("close"))
                if abs(float(sealed_close) - _obs_close) <= max(1e-9, abs(_obs_close) * 1e-9):
                    sealed_source_observation_id = source_id
            except (TypeError, ValueError, OverflowError):
                sealed_source_observation_id = ""
        _premodel_reason_codes = {
            "INSUFFICIENT_HISTORY", "FEATURE_SCHEMA_MISMATCH", "MODEL_NOT_AVAILABLE",
            "PRICE_BELOW_MINIMUM", "CROSS_NOT_STABLE_AT_SEAL",
        }
        _explicit_eligibility = prob.get("probability_eligible_for_model")
        _failure_code = str(prob.get("failure_reason_code") or "").strip().upper()
        probability_eligible_for_model = (
            bool(_explicit_eligibility)
            if _explicit_eligibility is not None
            else bool(prob.get("probability_attempted")) and _failure_code not in _premodel_reason_codes
        )
        probability_ineligible_reason = str(
            prob.get("probability_ineligible_reason_code")
            or (prob.get("failure_reason_code") if not probability_eligible_for_model else "")
            or ""
        )
        row: Dict[str, Any] = {
            "record_kind": "DECISION_ENRICHMENT", "dataset_name": "decision_enrichments",
            "event_type": str(event_type or "DECISION_ENRICHMENT"), "market_key": market,
            "market_family": _canonical_market_family(market),
            "session_date": _canonical_session_date_for_row({**obs, "signal_bar_time": signal_bar}, market), "symbol": symbol,
            "name": obs.get("name") or live.get("name") or paac.get("name"),
            "source_observation_id": source_id,
            "probability_source_observation_id": prob.get("probability_source_observation_id") or source_id,
            "source_observation_seq": obs.get("source_observation_seq"),
            "symbol_source_seq": self._source_seq_by_id.get(source_id), "symbol_source_seq_scope": "RECORDER_SESSION",
            "source_file": obs.get("source_file"), "source_mtime": obs.get("source_mtime"),
            "source_mtime_ns": obs.get("source_mtime_ns"), "source_size": obs.get("source_size"),
            "source_tail_sha256": obs.get("source_tail_sha256") or live.get("source_tail_sha256"),
            "source_snapshot_id": obs.get("source_snapshot_id") or live.get("source_snapshot_id"),
            "source_reconciliation_kind": obs.get("source_reconciliation_kind") or live.get("source_reconciliation_kind"),
            "exact_source_bar_materialized": (
                obs.get("exact_source_bar_materialized")
                if obs.get("exact_source_bar_materialized") is not None
                else live.get("exact_source_bar_materialized")
            ),
            "exact_source_bar_record_status": obs.get("exact_source_bar_record_status") or live.get("exact_source_bar_record_status"),
            "seal_trigger_source_observation_id": obs.get("seal_trigger_source_observation_id") or live.get("seal_trigger_source_observation_id"),
            "seal_trigger_bar_time": obs.get("seal_trigger_bar_time") or live.get("seal_trigger_bar_time"),
            "sealed_close_reconciled_from_observation_id": obs.get("sealed_close_reconciled_from_observation_id") or live.get("sealed_close_reconciled_from_observation_id"),
            "sealed_close_reconciled_previous_close": obs.get("sealed_close_reconciled_previous_close") or live.get("sealed_close_reconciled_previous_close"),
            "sealed_close_reconciliation_reason": obs.get("sealed_close_reconciliation_reason") or live.get("sealed_close_reconciliation_reason"),
            "source_snapshot_before_size": obs.get("source_snapshot_before_size") or live.get("source_snapshot_before_size"),
            "source_snapshot_before_mtime_ns": obs.get("source_snapshot_before_mtime_ns") or live.get("source_snapshot_before_mtime_ns"),
            "source_snapshot_before_tail_sha256": obs.get("source_snapshot_before_tail_sha256") or live.get("source_snapshot_before_tail_sha256"),
            "source_snapshot_after_size": obs.get("source_snapshot_after_size") or live.get("source_snapshot_after_size"),
            "source_snapshot_after_mtime_ns": obs.get("source_snapshot_after_mtime_ns") or live.get("source_snapshot_after_mtime_ns"),
            "source_snapshot_after_tail_sha256": obs.get("source_snapshot_after_tail_sha256") or live.get("source_snapshot_after_tail_sha256"),
            "regular_session_bar_close_1500": obs.get("regular_session_bar_close_1500") or live.get("regular_session_bar_close_1500"),
            "regular_session_bar_time_1500": obs.get("regular_session_bar_time_1500") or live.get("regular_session_bar_time_1500"),
            "official_session_close_1530": obs.get("official_session_close_1530") or live.get("official_session_close_1530"),
            "official_close_bar_time": obs.get("official_close_bar_time") or live.get("official_close_bar_time"),
            "official_close_finality_state": obs.get("official_close_finality_state") or live.get("official_close_finality_state"),
            "regular_session_close_reason_code": obs.get("regular_session_close_reason_code") or live.get("regular_session_close_reason_code"),
            "official_session_close_reason_code": obs.get("official_session_close_reason_code") or live.get("official_session_close_reason_code"),
            "official_close_used_for_model_signal": False,
            "official_close_used_for_outcomes": bool(obs.get("official_close_used_for_outcomes") or live.get("official_close_used_for_outcomes")),
            "source_detected_at": obs.get("source_detected_at"), "engine_received_at_utc": obs.get("engine_received_at_utc"),
            "observed_at": obs.get("observed_at"), "bar_datetime": obs.get("bar_datetime"),
            "bar_date": obs.get("bar_date"), "source_lane": obs.get("live_sniper_source_lane"),
            "positive_cross_now": obs.get("positive_cross_now"), "negative_cross_now": obs.get("negative_cross_now"),
            "p50_live": p50, "p100_live": p100, "probability_available": probability_available,
            "probability_scope": prob.get("probability_scope"), "probability_bar_time": prob.get("probability_bar_time") or signal_bar,
            "probability_asof": prob.get("probability_asof"), "probability_kind": prob.get("probability_kind"), **_a95_probability_record_fields(prob),
            "record_id": record_id, "decision_enrichment_id": record_id,
            "first_cross_time": first_cross_time,
            "decision_attempt_time": attempted, "decision_completed_time": completed,
            "current_bar_time": obs.get("bar_datetime"), "current_price": current_price,
            "first_cross_price": first_cross_price, "appearance_price": appearance_price,
            "appearance_time": appearance_time, "latest_update_at": completed,
            "data_state": (
                live.get("data_state")
                or ("مختوم" if live.get("signal_bar_sealed_at") or bool(live.get("signal_bar_sealed")) else "قيد الختم")
            ),
            "fast_confirmation_price": (live.get("fast_confirmation_price") if live.get("fast_confirmation_price") not in (None, "") else live.get("fast_confirm_price")),
            "fast_confirm_price": (live.get("fast_confirm_price") if live.get("fast_confirm_price") not in (None, "") else live.get("fast_confirmation_price")),
            "fast_confirm_at": live.get("fast_confirm_at") or live.get("fast_confirmation_at"),
            "probability_anchor_price": probability_anchor,
            "actual_entry_price": (live.get("actual_entry_price") if live.get("actual_entry_price") not in (None, "") else live.get("entry_price")),
            "sealed_signal_bar_close": sealed_close,
            "signal_bar_close": sealed_close,
            "signal_close": sealed_close,
            "sealed_source_bar_time": live.get("sealed_source_bar_time") or obs.get("sealed_source_bar_time"),
            "sealed_close_verified": bool(sealed_close is not None),
            "sealed_close_source": live.get("sealed_close_source") or obs.get("sealed_close_source") or ("EXACT_SOURCE_BAR" if sealed_close is not None else ""),
            "sealed_close_source_observation_id": sealed_source_observation_id,
            "sealed_close_verified_at": (
                live.get("sealed_close_verified_at") or obs.get("sealed_close_verified_at")
                or live.get("signal_bar_sealed_at") or obs.get("signal_bar_sealed_at") or ""
            ),
            "sealed_close_reason_code": "" if sealed_close is not None else sealed_close_reason,
            "probability_attempted": bool(
                prob.get(
                    "probability_attempted",
                    probability_available or bool(prob.get("probability_attempt_time")),
                )
            ),
            "probability_succeeded": bool(
                prob.get("probability_succeeded", False)
                and prob.get("probability_attempted", False)
            ),
            "probability_source": prob.get("probability_source") or prob.get("probability_kind") or "GANN20_PRODUCTION",
            "threshold_reached": bool(prob.get("threshold_reached")),
            "threshold_first_reached_at": prob.get("threshold_first_reached_at"),
            "probability_band": prob.get("probability_band"), "threshold_pct": prob.get("threshold_pct"),
            "radar_stage": live.get("radar_stage") or paac.get("pulse_acceptance_state"),
            "shadow_only": True, **rejects,
            "model_version": prob.get("model_version"), "model_sha256": prob.get("model_sha256"),
            "model_config_sha256": prob.get("model_config_sha256"), "model_r50_sha256": prob.get("model_r50_sha256"),
            "model_r100_sha256": prob.get("model_r100_sha256"), "feature_schema_sha256": prob.get("feature_schema_sha256"),
            "feature_count_expected": prob.get("feature_count_expected"), "feature_count_received": prob.get("feature_count_received"),
            "feature_contract_version": prob.get("feature_contract_version"), "time_contract_version": prob.get("time_contract_version"),
            "failure_stage": prob.get("failure_stage"), "failure_reason_code": prob.get("failure_reason_code"),
            "error_type": prob.get("error_type"), "error_message": prob.get("error_message"),
            "elapsed_ms": prob.get("elapsed_ms"),
            "cross_detected_at": live.get("cross_detected_at") or obs.get("observed_at"),
            "probability_completed_at": completed, "threshold_reached_at": prob.get("threshold_first_reached_at"),
            "signal_bar_sealed_at": live.get("signal_bar_sealed_at"), "ui_published_at": live.get("ui_published_at"),
            "source_to_cross_latency_ms": runtime.get("source_to_cross_latency_ms"),
            "cross_to_probability_latency_ms": prob.get("cross_to_probability_latency_ms", prob.get("live_sniper_cross_to_probability_ms")),
            "probability_to_threshold_latency_ms": runtime.get("probability_to_threshold_latency_ms"),
            "threshold_to_publish_latency_ms": runtime.get("threshold_to_publish_latency_ms"),
            "total_publish_latency_ms": runtime.get("total_publish_latency_ms"),
            "probability_compute_ms": prob.get("elapsed_ms", prob.get("live_sniper_probability_compute_ms")),
            "cross_to_probability_ms": prob.get("live_sniper_cross_to_probability_ms"),
            **{name: prob.get(name) for name in _PROBABILITY_TIMING_FIELDS},
            "candidate_ready_at": prob.get("candidate_ready_at"),
            "queue_entered_at": prob.get("queue_entered_at"),
            "worker_started_at": prob.get("worker_started_at"),
            "probability_cold_start": prob.get("probability_cold_start"),
            "probability_snapshot_cache_hit": prob.get("probability_snapshot_cache_hit"),
            "probability_snapshot_cache_key": prob.get("probability_snapshot_cache_key"),
            "probability_worker_request_id": prob.get("probability_worker_request_id"),
            "probability_worker_pid": prob.get("probability_worker_pid"),
            "probability_worker_generation": prob.get("probability_worker_generation"),
            "probability_timeout_budget_ms": prob.get("probability_timeout_budget_ms"),
            "probability_useful_sla_ms": prob.get("probability_useful_sla_ms"),
            "probability_sla_miss": prob.get("probability_sla_miss"),
            "probability_slowest_stage": prob.get("probability_slowest_stage"),
            "probability_feature_vector_sha256": prob.get("probability_feature_vector_sha256"),
            "probability_eligible_for_model": probability_eligible_for_model,
            "probability_ineligible_reason_code": probability_ineligible_reason,
            "anchor_price": probability_anchor, "r1_price": prob.get("r1_price", live.get("gann20_r1")),
            "r50_price": prob.get("r50_price", live.get("gann20_r50")),
            "r100_price": prob.get("r100_price", live.get("gann20_r100")),
            "stop_price": prob.get("stop_price", live.get("gann20_stop")),
            "paac_state": paac.get("pulse_acceptance_state"),
            "paac_episode_id": paac.get("pulse_episode_id") or paac.get("id"),
            "pulse_episode_id": identity["episode_id"], "signal_bar_time": signal_bar,
            "live_event_type": live.get("live_sniper_event_type") or live.get("event_type"),
            "live_birth_proven": live.get("live_sniper_birth_proven"), "live_birth_kind": live.get("live_sniper_birth_kind"),
            "live_born_at": live.get("live_sniper_born_at"), "signal_entry_price": live.get("signal_entry_price") or appearance_price,
            "execution_passed": live.get("execution_passed"),
            "execution_rule_passed": live.get("execution_rule_passed", live.get("execution_passed")),
            "execution_shadow_passed": live.get("execution_shadow_passed"),
            "execution_authority": live.get("execution_authority") or "SHADOW_ONLY",
            "execution_authorized": False, "execution_publishable": live.get("execution_publishable"),
            "execution_decision": live.get("execution_decision"), "execution_profile": live.get("execution_profile"),
            "execution_tier": live.get("execution_tier"), "execution_blockers": live.get("execution_blockers"),
            "entry_status_code": live.get("entry_status_code"),
            "target_consumed_before_entry": live.get("target_consumed_before_entry"),
            "execution_decision_ar": live.get("execution_decision_ar"),
            "trader_queue_bucket": live.get("trader_queue_bucket"),
            "trace_id": trace_id, **identity,
            "extra_json": {"runtime": runtime},
        }
        row = _truth_apply_trader_columns(row, for_ui=False)
        row = _truth_annotate_sealed_close_claim(row, source_observation=obs)
        _decision_write_ms = round((time.perf_counter() - _decision_write_t0) * 1000.0, 3)
        _timing_seed = {name: row.get(name) for name in _PROBABILITY_TIMING_FIELDS}
        _timing_seed["decision_write_ms"] = _decision_write_ms
        try:
            _prior_total_ms = max(0.0, float(prob.get("total_probability_pipeline_ms") or 0.0))
        except (TypeError, ValueError, OverflowError):
            _prior_total_ms = 0.0
        _total_with_decision_ms = _prior_total_ms + _decision_write_ms
        row.update(_finalize_probability_timing(_timing_seed, total_ms=_total_with_decision_ms))
        try:
            _useful_sla_ms = float(prob.get("probability_useful_sla_ms"))
            if not math.isfinite(_useful_sla_ms) or _useful_sla_ms <= 0.0:
                raise ValueError("invalid probability_useful_sla_ms")
        except (TypeError, ValueError, OverflowError):
            _useful_sla_ms = _env_float("AIN_PROBABILITY_USEFUL_SLA_MS", 5000.0, 100.0, 60000.0)
        _sla_miss = bool(
            prob.get("probability_sla_miss")
            or (probability_available and probability_eligible_for_model and _total_with_decision_ms > _useful_sla_ms)
        )
        row["probability_timing_complete"] = _probability_timing_row_complete(row)
        row["probability_useful_sla_ms"] = _useful_sla_ms
        row["probability_sla_miss"] = _sla_miss
        row["probability_within_useful_sla"] = bool(
            probability_available
            and probability_eligible_for_model
            and not _sla_miss
            and _total_with_decision_ms <= _useful_sla_ms
        )
        prepared = self._prepare_row(row)
        with self._lock:
            if not self._reserve_decision_id_locked(record_id):
                log_event(
                    "DECISION_DEDUPED",
                    "تم تجاهل إعادة كتابة القرار نفسه",
                    session_id=self.session_id,
                    trace_id=trace_id,
                    episode_id=identity["episode_id"],
                    symbol=symbol,
                    decision_enrichment_id=record_id,
                )
                return True
        accepted = self._enqueue_prepared(prepared)
        if not accepted:
            with self._lock:
                self._seen_decision_ids.pop(record_id, None)
            self._record_error("decision_write", "enqueue_decision_enrichment", "DECISION_WAL_ENQUEUE_FAILED", row=prepared, reason_code="DECISION_WAL_ENQUEUE_FAILED")
            return False
        with self._lock:
            self._stats["decision_enrichments"] += 1
            self._episode_last_decision_id[identity["episode_id"]] = record_id
        log_event("DECISION_WRITE", "تمت كتابة محاولة القرار", session_id=self.session_id, trace_id=trace_id, episode_id=identity["episode_id"], symbol=symbol, probability_available=probability_available, rejection_reason_code=rejects.get("rejection_reason_code", ""))
        if write_context:
            with self._lock:
                context_errors_before = int(self._stats.get("errors_by_stage", {}).get("context_write", 0))
            context_ok = self.record_decision_context_snapshot(
                market_key=market, observation=obs, probability=prob, live_row={**live, "decision_enrichment_id": record_id, **identity},
                runtime_context={**runtime, "decision_enrichment_id": record_id, "trace_id": trace_id},
                event_type="DECISION_CONTEXT_SNAPSHOT",
            )
            if not context_ok:
                with self._lock:
                    context_errors_after = int(self._stats.get("errors_by_stage", {}).get("context_write", 0))
                    self._stats["decision_partial_context_failures"] = int(
                        self._stats.get("decision_partial_context_failures", 0)
                    ) + 1
                # A monkeypatched/foreign context writer may return False without
                # classifying its own failure. Guarantee one visible error, but do
                # not double-count when the canonical writer already did so.
                if context_errors_after == context_errors_before:
                    self._record_error(
                        "context_write", "mandatory_context_for_decision",
                        "MANDATORY_CONTEXT_WRITE_FAILED", row=prepared,
                        reason_code="MANDATORY_CONTEXT_WRITE_FAILED",
                    )
                else:
                    try:
                        self._persist_stats_snapshot_atomic()
                    except Exception as persist_exc:
                        logging.getLogger("FullTickResearchRecorder").error(
                            "تعذر تثبيت حالة فشل السياق الجزئي: %s: %s",
                            type(persist_exc).__name__, persist_exc, exc_info=True,
                        )
                log_event(
                    "DECISION_WRITE_PARTIAL",
                    "كُتب القرار وتعذرت لقطة السياق؛ تم حفظ الحقيقة الجزئية وتصنيف الفشل",
                    level=logging.WARNING, session_id=self.session_id, trace_id=trace_id,
                    episode_id=identity["episode_id"], symbol=symbol,
                    reason_code="MANDATORY_CONTEXT_WRITE_FAILED",
                )
                # The decision itself is durable and must not be retried as if it
                # vanished. Readiness will remain false because context coverage
                # and context_write counters expose the partial chain.
                return True
        return True

    def record_decision_context_snapshot(
        self, *, market_key: str, observation: Mapping[str, Any],
        probability: Optional[Mapping[str, Any]] = None, live_row: Optional[Mapping[str, Any]] = None,
        runtime_context: Optional[Mapping[str, Any]] = None,
        active_rows: Optional[Iterable[Mapping[str, Any]]] = None,
        event_type: str = "DECISION_CONTEXT_SNAPSHOT",
    ) -> bool:
        obs, prob, live, runtime = map(dict, (observation or {}, probability or {}, live_row or {}, runtime_context or {}))
        try:
            from decision_context_snapshot import build_decision_context_snapshot
            context = build_decision_context_snapshot(
                market_key=str(market_key or ""), observation=obs, probability=prob,
                live_row=live, runtime_context=runtime, active_rows=active_rows,
            )
        except Exception as exc:
            self._record_error("context_write", "build_decision_context_snapshot", exc, row={**obs, **live}, reason_code="CONTEXT_BUILD_FAILED")
            return False
        if not context.get("symbol") or not context.get("source_observation_id"):
            self._record_error("context_write", "validate_decision_context", "CONTEXT_LINK_FIELDS_MISSING", row={**obs, **live}, reason_code="CONTEXT_LINK_FIELDS_MISSING")
            return False
        market = canonical_runtime_key(market_key or context.get("market_key"))
        signal_bar = context.get("signal_bar_time") or obs.get("bar_datetime")
        identity = episode_identity(market, context.get("symbol"), signal_bar)
        decision_id = str(
            runtime.get("decision_enrichment_id") or live.get("decision_enrichment_id")
            or self._episode_last_decision_id.get(identity["episode_id"]) or ""
        )
        context_id = str(context.get("decision_context_id") or f"CTX-{identity['episode_key_sha256'][:24]}-{hashlib.sha256((str(context.get('source_observation_id'))+str(context.get('decision_observed_at'))).encode()).hexdigest()[:10]}")
        row = {
            "record_kind": "DECISION_CONTEXT_SNAPSHOT", "dataset_name": "decision_context_snapshots",
            "event_type": str(event_type or "DECISION_CONTEXT_SNAPSHOT"),
            "source_observation_seq": obs.get("source_observation_seq"),
            "symbol_source_seq": self._source_seq_by_id.get(str(context.get("source_observation_id") or "")),
            "symbol_source_seq_scope": "RECORDER_SESSION", "decision_context_id": context_id,
            "decision_enrichment_id": decision_id, "trace_id": runtime.get("trace_id") or prob.get("trace_id"),
            **identity, **context,
            "extra_json": {"runtime": runtime},
        }
        prepared = self._prepare_row(row)
        accepted = self._enqueue_prepared(prepared)
        if accepted:
            with self._lock:
                self._stats["decision_context_snapshots"] += 1
            log_event("CONTEXT_WRITE", "تمت كتابة لقطة سياق القرار", session_id=self.session_id, trace_id=prepared.get("trace_id"), episode_id=prepared.get("episode_id"), symbol=prepared.get("symbol"), context_complete=prepared.get("context_complete"))
            return True
        self._record_error("context_write", "enqueue_context_snapshot", "CONTEXT_WAL_ENQUEUE_FAILED", row=prepared, reason_code="CONTEXT_WAL_ENQUEUE_FAILED")
        return False

    def record_event_update(
        self, *, market_key: str, observation: Mapping[str, Any],
        event_row: Mapping[str, Any], event_type: str,
    ) -> bool:
        obs, event = dict(observation or {}), dict(event_row or {})
        source_id = str(obs.get("source_observation_id") or event.get("source_observation_id") or "")
        symbol = str(obs.get("symbol") or event.get("symbol") or "").strip().upper()
        market = canonical_runtime_key(market_key or obs.get("market_key") or event.get("market_key"))
        signal_bar = obs.get("episode_signal_bar_time") or event.get("episode_signal_bar_time") or event.get("signal_bar_time") or event.get("probability_bar_time") or obs.get("bar_datetime")
        if not source_id or not symbol or not signal_bar:
            self._record_error("event_write", "record_event_update", "EVENT_LINK_FIELDS_MISSING", row={**obs, **event, "market_key": market}, reason_code="EVENT_LINK_FIELDS_MISSING")
            return False
        identity = episode_identity(market, symbol, signal_bar)
        event_name = str(event_type or event.get("event_type") or "EVENT_UPDATE").upper()
        event_time = event.get("event_time") or event.get("probability_completed_time") or obs.get("observed_at") or _utc_now_iso()
        event_id = str(event.get("event_id") or f"EVT-{identity['episode_key_sha256'][:20]}-{hashlib.sha256((event_name+source_id+str(event_time)).encode()).hexdigest()[:12]}")
        row = {
            "record_kind": "EVENT_UPDATE", "dataset_name": "event_updates",
            "event_type": event_name, "market_key": market, "market_family": _canonical_market_family(market),
            "session_date": _canonical_session_date_for_row({**obs, "signal_bar_time": signal_bar}, market), "symbol": symbol,
            "name": obs.get("name") or event.get("name"), "source_observation_id": source_id,
            "source_observation_seq": obs.get("source_observation_seq"),
            "symbol_source_seq": self._source_seq_by_id.get(source_id), "symbol_source_seq_scope": "RECORDER_SESSION",
            "source_file": obs.get("source_file"), "source_mtime": obs.get("source_mtime"),
            "source_mtime_ns": obs.get("source_mtime_ns"), "source_size": obs.get("source_size"),
            "source_tail_sha256": obs.get("source_tail_sha256") or event.get("source_tail_sha256"),
            "source_snapshot_id": obs.get("source_snapshot_id") or event.get("source_snapshot_id"),
            "source_reconciliation_kind": obs.get("source_reconciliation_kind") or event.get("source_reconciliation_kind"),
            "exact_source_bar_materialized": obs.get("exact_source_bar_materialized") or event.get("exact_source_bar_materialized"),
            "regular_session_bar_close_1500": obs.get("regular_session_bar_close_1500") or event.get("regular_session_bar_close_1500"),
            "official_session_close_1530": obs.get("official_session_close_1530") or event.get("official_session_close_1530"),
            "official_close_bar_time": obs.get("official_close_bar_time") or event.get("official_close_bar_time"),
            "official_close_finality_state": obs.get("official_close_finality_state") or event.get("official_close_finality_state"),
            "source_detected_at": obs.get("source_detected_at"), "engine_received_at_utc": obs.get("engine_received_at_utc"),
            "observed_at": obs.get("observed_at"), "bar_datetime": obs.get("bar_datetime"),
            "bar_date": obs.get("bar_date"), "source_lane": obs.get("live_sniper_source_lane"),
            "price": event.get("price", obs.get("close", event.get("current_price"))),
            "pulse_episode_id": identity["episode_id"], "signal_bar_time": signal_bar,
            "live_sniper_event_type": event.get("live_sniper_event_type") or event_name,
            "event_id": event_id, "event_time": event_time,
            "p50": event.get("p50", event.get("p50_pct", event.get("p50_live"))),
            "p100": event.get("p100", event.get("p100_pct", event.get("p100_live"))),
            "previous_state": event.get("previous_state"), "new_state": event.get("new_state"),
            "reason_code": event.get("reason_code") or event.get("failure_reason_code"),
            "append_only": True, "execution_shadow_passed": event.get("execution_shadow_passed"),
            "execution_authority": event.get("execution_authority") or "SHADOW_ONLY",
            "execution_authorized": False, "trace_id": event.get("trace_id"),
            **identity, "event_payload_json": event, "extra_json": {},
        }
        prepared = self._prepare_row(row)
        accepted = self._enqueue_prepared(prepared)
        if accepted:
            with self._lock:
                self._stats["event_updates"] += 1
            log_event("EVENT_WRITE", f"تمت كتابة حدث {event_name}", session_id=self.session_id, trace_id=prepared.get("trace_id"), episode_id=prepared.get("episode_id"), symbol=symbol, event_id=event_id)
            # A material transition is persisted through the canonical engine
            # bridge with write_decision=True.  That decision writes its own
            # context.  Reusing the previous decision id here with the current
            # event observation created duplicate and source-mismatched contexts.
            return True
        self._record_error("event_write", "enqueue_event_update", "EVENT_WAL_ENQUEUE_FAILED", row=prepared, reason_code="EVENT_WAL_ENQUEUE_FAILED")
        return False

    def _new_segment(self, partition: Tuple[str, str, str, str]) -> _Segment:
        sequence = self._next_segment_seq()
        directory = self._partition_dir(self.wal_root, partition)
        path = directory / f"segment-{self.session_id}-{sequence:06d}.jsonl.open"
        handle = path.open("a", encoding="utf-8", buffering=256 * 1024)
        now = time.time()
        return _Segment(partition, path, handle, 0, 0, now, now, now, sequence)

    def _sync_segment(self, segment: _Segment) -> None:
        if not segment.unfsynced_seqs:
            return
        segment.handle.flush()
        os.fsync(segment.handle.fileno())
        self._mark_fsynced(segment.unfsynced_seqs)
        segment.unfsynced_seqs.clear()
        segment.last_fsync_at = time.time()
        segment.last_flush_at = segment.last_fsync_at

    def _close_segment(self, partition: Tuple[str, str, str, str]) -> None:
        segment = self._segments.pop(partition, None)
        if segment is None:
            return
        try:
            self._sync_segment(segment)
            segment.handle.close()
            closed_path = segment.open_path.with_suffix(".closed")
            if _env_bool("AIN_FREEZE_SAMPLE_REQUIRED", False):
                replace_with_retry(
                    segment.open_path, closed_path, delays=DURABLE_STATE_REPLACE_DELAYS,
                    write_through=True,
                )
                sync_file(closed_path)
                sync_parent_directory(closed_path)
            else:
                os.replace(segment.open_path, closed_path)
            with self._lock:
                self._stats["closed_segments"] += 1
            self._enqueue_conversion(closed_path)
        except Exception as exc:
            self._record_error("wal_fsync", "close_segment", exc, row={"market_key": partition[1]}, reason_code="WAL_SEGMENT_CLOSE_FAILED")

    @staticmethod
    def _critical_durability_row(row: Mapping[str, Any]) -> bool:
        if str(row.get("record_kind") or "") in {"DECISION_ENRICHMENT", "EVENT_UPDATE", "DECISION_CONTEXT_SNAPSHOT"}:
            return True
        return bool(
            row.get("source_journal_durability_required")
            or row.get("positive_cross_now")
            or row.get("negative_cross_now")
        )

    def _write_row(self, row: Dict[str, Any]) -> None:
        partition = self._partition_for(row)
        segment = self._segments.get(partition)
        if segment is None:
            segment = self._new_segment(partition)
            self._segments[partition] = segment
        line = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        segment.handle.write(line)
        segment.rows += 1
        segment.bytes_written += len(line.encode("utf-8"))
        seq = int(row.get("event_seq") or 0)
        segment.unfsynced_seqs.append(seq)
        self._mark_written(seq)
        now = time.time()
        with self._durability_condition:
            force_durable = seq in self._force_durable_seqs
        if self._critical_durability_row(row) or force_durable:
            self._sync_segment(segment)
        else:
            if now - segment.last_flush_at >= self.flush_seconds:
                segment.handle.flush()
                segment.last_flush_at = now
            if now - segment.last_fsync_at >= self.fsync_seconds:
                self._sync_segment(segment)
        if segment.rows >= self.segment_rows or segment.bytes_written >= self.segment_bytes or now - segment.opened_at >= self.segment_seconds:
            self._close_segment(partition)

    def _rotate_idle_segments(self) -> None:
        now = time.time()
        for partition, segment in list(self._segments.items()):
            if now - segment.opened_at >= self.segment_seconds and now - segment.last_flush_at >= min(self.flush_seconds, 1.0):
                self._close_segment(partition)

    def _writer_loop(self) -> None:
        try:
            while True:
                self._write_lease()
                self._service_force_durable()
                if self._stop.is_set() and self._queue.empty() and self._critical_queue.empty():
                    break
                source_queue = self._critical_queue
                try:
                    item = source_queue.get_nowait()
                except queue.Empty:
                    source_queue = self._queue
                    try:
                        item = source_queue.get_nowait()
                    except queue.Empty:
                        # Do not block on the normal queue: that would delay a
                        # critical source fact that arrives just after get().
                        # Clear-then-recheck closes the wakeup race.
                        self._writer_wakeup.clear()
                        if not self._critical_queue.empty() or not self._queue.empty():
                            continue
                        self._writer_wakeup.wait(timeout=0.10)
                        self._service_force_durable()
                        self._rotate_idle_segments(); self._write_stats_if_due()
                        continue
                try:
                    if item is not None:
                        self._write_row(item); self._write_stats_if_due()
                except Exception as exc:
                    if isinstance(item, dict):
                        self._mark_durability_failed(int(item.get("event_seq") or 0), "WAL_WRITE_FAILED")
                    self._record_error("wal_write", "writer_loop_write_row", exc, row=item or {}, reason_code="WAL_WRITE_FAILED")
                finally:
                    try:
                        source_queue.task_done()
                    except Exception as exc:
                        self._record_error(
                            "wal_write", "writer_queue_task_done", exc,
                            row=item or {}, reason_code="WAL_QUEUE_ACCOUNTING_FAILED",
                        )
        finally:
            for partition in list(self._segments):
                self._close_segment(partition)
            self._close_overflow_handles()
            self._write_stats_if_due(force=True)

    def _close_overflow_handles(self) -> None:
        with self._lock:
            items = list(self._overflow_handles.items())
            self._overflow_handles.clear()
        for partition, segment in items:
            lock = self._overflow_lock_for(partition)
            try:
                with lock:
                    segment.handle.flush()
                    os.fsync(segment.handle.fileno())
                    segment.handle.close()
                    closed = segment.open_path.with_suffix(".closed")
                    if _env_bool("AIN_FREEZE_SAMPLE_REQUIRED", False):
                        replace_with_retry(
                            segment.open_path, closed, delays=DURABLE_STATE_REPLACE_DELAYS,
                            write_through=True,
                        )
                        sync_file(closed)
                        sync_parent_directory(closed)
                    else:
                        os.replace(segment.open_path, closed)
                with self._lock:
                    self._stats["closed_segments"] += 1
                self._enqueue_conversion(closed)
            except Exception as exc:
                self._record_error("wal_fsync", "close_overflow_segment", exc, row={"market_key": partition[1]}, reason_code="WAL_OVERFLOW_CLOSE_FAILED")

    def _conversion_marker_path(self, wal_path: Path) -> Path:
        return wal_path.with_name(wal_path.name + ".parquet.done.json")

    def _conversion_complete(self, wal_path: Path) -> bool:
        marker = self._conversion_marker_path(wal_path)
        if marker.exists():
            try:
                payload = json.loads(marker.read_text(encoding="utf-8"))
                specs = payload.get("parquet_outputs") or payload.get("outputs") or []
                outputs: List[Path] = []
                for item in specs:
                    if isinstance(item, Mapping):
                        path = self._resolve_marker_path(item.get("relative_path") or item.get("path"), fallback_name=str(item.get("file_name") or ""))
                        expected_sha = str(item.get("file_sha256") or item.get("sha256") or "")
                    else:
                        path = self._resolve_marker_path(item, fallback_name=Path(str(item)).name)
                        expected_sha = ""
                    if path is None or path.stat().st_size <= 0:
                        return False
                    if expected_sha and _sha256_file(path) != expected_sha:
                        return False
                    outputs.append(path)
                wal_sha = str(payload.get("wal_sha256") or "")
                if wal_sha and _sha256_file(wal_path) != wal_sha:
                    return False
                return bool(outputs)
            except Exception as exc:
                self._record_error("provenance_write", "validate_conversion_marker", exc, row={"source_file": str(wal_path)}, reason_code="CONVERSION_MARKER_INVALID")
                return False
        if not self._uses_default_parquet_writer and self._parquet_output_path(wal_path).exists():
            return True
        return False

    def _cleanup_retention(self) -> None:
        # R169.2 never deletes WAL or conversion certificates automatically.
        # Manual retention tooling may be added later, but it must not run inside
        # the live process and cannot be triggered by age alone.
        return

    def _relative_path(self, path: Path) -> str:
        try:
            return str(Path(path).resolve().relative_to(self.base_dir.resolve())).replace("\\", "/")
        except Exception:
            return Path(path).name

    def _resolve_marker_path(self, value: Any, *, fallback_name: str = "") -> Optional[Path]:
        text = str(value or "").strip()
        candidates = []
        if text:
            raw = Path(text)
            candidates.append(raw if raw.is_absolute() else self.base_dir / raw)
        if fallback_name:
            candidates.extend(self.base_dir.rglob(fallback_name))
        for candidate in candidates:
            try:
                if candidate.exists():
                    return candidate.resolve()
            except Exception:
                continue
        return None

    def _parquet_output_path(self, wal_path: Path, dataset: Optional[str] = None) -> Path:
        rel = wal_path.relative_to(self.wal_root)
        parts = list(rel.parts)
        if parts and parts[0].startswith("dataset="):
            directory = self.parquet_root.joinpath(*parts[:-1])
        else:
            dataset = dataset or "legacy_mixed"
            directory = self.parquet_root / f"dataset={dataset}" / Path(*parts[:-1])
        directory.mkdir(parents=True, exist_ok=True)
        name = wal_path.name.replace(".jsonl.closed", ".parquet")
        name = name.replace("segment-", "part-").replace("overflow-", "overflow-part-")
        return directory / name

    def _write_arrow_part(self, dataset: str, rows: List[Dict[str, Any]], out_path: Path) -> Tuple[int, int]:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore

        type_map = {"string": pa.string(), "bool": pa.bool_(), "int64": pa.int64(), "float64": pa.float64()}
        fields = [pa.field(str(c["name"]), type_map[str(c["type"])], nullable=True) for c in _dataset_columns(dataset)]
        metadata = {
            b"ain_schema_version": str(SCHEMA_VERSION).encode(),
            b"ain_dataset": dataset.encode(),
            b"ain_schema_fingerprint": _schema_fingerprint(dataset).encode(),
            b"ain_recorder_version": VERSION.encode(),
        }
        schema = pa.schema(fields, metadata=metadata)
        normalized = [normalize_row_for_dataset(row, dataset) for row in rows]
        table = pa.Table.from_pylist(normalized, schema=schema)
        tmp = out_path.with_suffix(out_path.suffix + ".tmp")
        pq.write_table(table, tmp, compression=self.compression, use_dictionary=True, write_statistics=True)
        if _env_bool("AIN_FREEZE_SAMPLE_REQUIRED", False):
            # The frozen Parquet projection is physically attested, so its name
            # must survive the same sudden-reset boundary as the WAL and marker.
            replace_with_retry(tmp, out_path, delays=DURABLE_STATE_REPLACE_DELAYS, write_through=True)
            sync_file(out_path)
            sync_parent_directory(out_path)
        else:
            os.replace(tmp, out_path)
        return int(table.num_rows), int(out_path.stat().st_size)

    def _convert_wal_fixed_schema(self, wal_path: Path) -> Tuple[int, int, List[Path]]:
        groups: Dict[str, List[Dict[str, Any]]] = {}
        with wal_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                text = line.strip()
                if not text:
                    continue
                row = json.loads(text)
                groups.setdefault(_dataset_for_row(row), []).append(row)
        if not groups:
            return 0, 0, []
        engine = parquet_engine_name()
        if not engine:
            raise RuntimeError("PyArrow is required for fixed-schema Parquet compaction")
        total_rows = total_bytes = 0
        outputs: List[Path] = []
        for dataset, rows in sorted(groups.items()):
            out_path = self._parquet_output_path(wal_path, dataset)
            if len(groups) > 1 and f"dataset={dataset}" not in str(out_path.parent):
                out_path = out_path.with_name(out_path.stem + f"-{dataset}" + out_path.suffix)
            count, size = self._write_arrow_part(dataset, rows, out_path)
            total_rows += count
            total_bytes += size
            outputs.append(out_path.resolve())
        return total_rows, total_bytes, outputs

    def _convert_one(self, wal_path: Path) -> bool:
        if not self.parquet_enabled:
            return False
        if self._conversion_complete(wal_path):
            return True
        if not parquet_engine_available() and self._uses_default_parquet_writer:
            with self._lock:
                self._stats["parquet_deferred_no_engine"] += 1
            return False
        started_at = _utc_now_iso()
        try:
            wal_rows = sum(1 for line in wal_path.open("r", encoding="utf-8") if line.strip())
            wal_sha = _sha256_file(wal_path)
            if self._uses_default_parquet_writer:
                rows, size, outputs = self._convert_wal_fixed_schema(wal_path)
            else:
                out_path = self._parquet_output_path(wal_path)
                rows, size = self._parquet_writer(wal_path, out_path)  # type: ignore[misc]
                outputs = [out_path.resolve()] if rows > 0 else []
            if rows <= 0 or not outputs:
                raise RuntimeError("PARQUET_CONVERSION_EMPTY_OUTPUT")
            output_specs = []
            formal_freeze = _env_bool("AIN_FREEZE_SAMPLE_REQUIRED", False)
            for output in outputs:
                path = Path(output).resolve()
                if formal_freeze:
                    # Parquet is a rebuildable projection, but a frozen sample
                    # attests its physical projection too. Force the final output
                    # and its directory entry before writing the durable marker so
                    # a sudden reset cannot leave a green health marker pointing
                    # at a projection that only existed in the page cache.
                    sync_file(path)
                    sync_parent_directory(path)
                output_specs.append({
                    "relative_path": self._relative_path(path), "file_name": path.name,
                    "file_sha256": _sha256_file(path), "bytes": int(path.stat().st_size),
                })
            marker_payload = {
                "research_root_id": hashlib.sha256(str(self.base_dir.name).encode()).hexdigest()[:20],
                "dataset_name": _dataset_for_row(json.loads(next(line for line in wal_path.read_text(encoding="utf-8").splitlines() if line.strip()))),
                "session_id": self.session_id, "schema_version": SCHEMA_VERSION,
                "wal_relative_path": self._relative_path(wal_path), "wal_file_name": wal_path.name,
                "wal_sha256": wal_sha, "wal_rows": int(wal_rows),
                "parquet_outputs": output_specs, "parquet_rows": int(rows),
                "parquet_bytes": int(size), "conversion_started_at": started_at,
                "conversion_completed_at": _utc_now_iso(),
                "pyarrow_version": __import__("pyarrow").__version__ if parquet_engine_available() else "custom_writer",
            }
            marker = self._conversion_marker_path(wal_path)
            if formal_freeze:
                write_json_atomic(
                    marker, marker_payload, indent=2, fsync_file=True,
                    fsync_directory=True, allow_nan=False,
                )
            else:
                tmp = marker.with_suffix(marker.suffix + ".tmp")
                tmp.write_text(json.dumps(marker_payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
                os.replace(tmp, marker)
            with self._lock:
                self._stats["parquet_parts"] += len(outputs)
                self._stats["parquet_rows"] += int(rows)
                self._stats["parquet_bytes"] += int(size)
            log_event("PARQUET_CONVERSION", "اكتمل تحويل WAL إلى Parquet", session_id=self.session_id, wal=marker_payload["wal_relative_path"], rows=rows, outputs=output_specs)
            return True
        except Exception as exc:
            with self._lock:
                self._stats["conversion_errors"] += 1
            self._record_error("parquet_conversion", "convert_wal_to_parquet", exc, row={"source_file": str(wal_path)}, reason_code="PARQUET_CONVERSION_FAILED")
            return False

    def _converter_loop(self) -> None:
        try:
            while True:
                self._write_lease()
                now = time.time()
                if now - self._last_recovery_scan >= self.recovery_scan_seconds:
                    self._last_recovery_scan = now
                    self._recover_orphan_segments()
                if self._converter_stop.is_set() and self._convert_queue.empty():
                    break
                try:
                    path = self._convert_queue.get(timeout=0.20)
                except queue.Empty:
                    continue
                key = str(Path(path).resolve()) if path is not None else ""
                try:
                    if path is not None:
                        self._convert_one(Path(path))
                finally:
                    with self._lock:
                        self._conversion_pending.discard(key)
                    try:
                        self._convert_queue.task_done()
                    except Exception as exc:
                        self._record_error(
                            "parquet_conversion", "converter_queue_task_done", exc,
                            row={"source_file": str(path or "")},
                            reason_code="PARQUET_QUEUE_ACCOUNTING_FAILED",
                        )
        finally:
            self._cleanup_retention()
            self._write_stats()

    def validate_frozen_sample_storage(self) -> Dict[str, Any]:
        """Re-open the durable research store and prove WAL→Parquet conservation.

        Runtime counters alone are not sufficient for a frozen sample: a file
        could be missing, truncated or replaced after the converter incremented
        its in-memory counters.  This validator re-hashes every closed WAL and
        every conversion output from disk.  With the production PyArrow writer
        it also re-opens Parquet metadata and verifies the signed fixed-schema
        dataset fingerprint and physical row count.
        """
        errors: List[str] = []
        wal_files = sorted(self.wal_root.rglob("*.jsonl.closed")) if self.wal_root.exists() else []
        open_wal = sorted(self.wal_root.rglob("*.jsonl.open")) if self.wal_root.exists() else []
        tmp_files = []
        for root in (self.wal_root, self.parquet_root):
            if root.exists():
                tmp_files.extend(sorted(p for p in root.rglob("*") if p.is_file() and ".tmp" in p.name))
        if open_wal:
            errors.append("RESEARCH_WAL_OPEN_SEGMENTS_REMAIN")
        if tmp_files:
            errors.append("RESEARCH_TEMP_ARTIFACTS_REMAIN")
        if not wal_files:
            errors.append("RESEARCH_CLOSED_WAL_MISSING")

        total_wal_rows = 0
        total_marker_rows = 0
        total_parquet_rows_physical = 0
        cohort_mismatch_rows = 0
        invalid_json_rows = 0
        markers_valid = 0
        output_files = 0
        marker_details: List[Dict[str, Any]] = []
        sample_entries_by_path: Dict[str, Dict[str, Any]] = {}

        def _remember_sample_file(path: Path, digest: Optional[str] = None) -> None:
            rel = self._relative_path(path)
            sample_entries_by_path[rel] = {
                "path": rel, "size": int(path.stat().st_size),
                "sha256": str(digest or _sha256_file(path)).lower(),
            }

        expected_marker_paths = {self._conversion_marker_path(wal).resolve() for wal in wal_files}
        actual_marker_paths = {p.resolve() for p in self.wal_root.rglob("*.parquet.done.json")} if self.wal_root.exists() else set()
        extra_markers = sorted(str(p) for p in (actual_marker_paths - expected_marker_paths))
        if extra_markers:
            errors.append("RESEARCH_UNREFERENCED_CONVERSION_MARKERS_PRESENT")
        referenced_outputs: set[Path] = set()
        for wal_path in wal_files:
            item: Dict[str, Any] = {"wal": self._relative_path(wal_path), "errors": []}
            try:
                wal_sha = _sha256_file(wal_path)
                _remember_sample_file(wal_path, wal_sha)
                wal_rows = 0
                with wal_path.open("r", encoding="utf-8") as wal_handle:
                    for line_no, line in enumerate(wal_handle, start=1):
                        if not line.strip():
                            continue
                        wal_rows += 1
                        try:
                            row_payload = json.loads(line)
                        except Exception:
                            invalid_json_rows += 1
                            item["errors"].append("WAL_JSON_INVALID")
                            errors.append(f"RESEARCH_WAL_JSON_INVALID:{item['wal']}:{line_no}")
                            continue
                        if str(row_payload.get("recorder_session_id") or "") != self.session_id:
                            cohort_mismatch_rows += 1
                            item["errors"].append("RECORDER_SESSION_MISMATCH")
                            errors.append(f"RESEARCH_RECORDER_SESSION_MISMATCH:{item['wal']}:{line_no}")
                total_wal_rows += int(wal_rows)
                marker_path = self._conversion_marker_path(wal_path)
                item["wal_sha256"] = wal_sha
                item["wal_rows"] = int(wal_rows)
                item["marker"] = self._relative_path(marker_path)
                if not marker_path.is_file():
                    item["errors"].append("CONVERSION_MARKER_MISSING")
                    errors.append(f"RESEARCH_CONVERSION_MARKER_MISSING:{item['wal']}")
                    marker_details.append(item)
                    continue
                marker_bytes = marker_path.read_bytes()
                _remember_sample_file(marker_path, hashlib.sha256(marker_bytes).hexdigest())
                marker = json.loads(marker_bytes.decode("utf-8"))
                if str(marker.get("session_id") or "") != self.session_id:
                    item["errors"].append("MARKER_SESSION_MISMATCH")
                    errors.append(f"RESEARCH_CONVERSION_MARKER_SESSION_MISMATCH:{item['wal']}")
                if int(marker.get("schema_version") or -1) != int(SCHEMA_VERSION):
                    item["errors"].append("MARKER_SCHEMA_VERSION_MISMATCH")
                    errors.append(f"RESEARCH_CONVERSION_MARKER_SCHEMA_MISMATCH:{item['wal']}")
                if str(marker.get("wal_relative_path") or "").replace("\\", "/") != self._relative_path(wal_path):
                    item["errors"].append("MARKER_WAL_PATH_MISMATCH")
                    errors.append(f"RESEARCH_CONVERSION_MARKER_WAL_PATH_MISMATCH:{item['wal']}")
                if str(marker.get("wal_sha256") or "").lower() != wal_sha:
                    item["errors"].append("WAL_SHA_MISMATCH")
                    errors.append(f"RESEARCH_WAL_MARKER_SHA_MISMATCH:{item['wal']}")
                if int(marker.get("wal_rows") or -1) != int(wal_rows):
                    item["errors"].append("WAL_ROW_COUNT_MISMATCH")
                    errors.append(f"RESEARCH_WAL_MARKER_ROW_MISMATCH:{item['wal']}")
                marker_rows = int(marker.get("parquet_rows") or 0)
                total_marker_rows += marker_rows
                specs = list(marker.get("parquet_outputs") or marker.get("outputs") or [])
                if not specs:
                    item["errors"].append("PARQUET_OUTPUTS_EMPTY")
                    errors.append(f"RESEARCH_PARQUET_OUTPUTS_EMPTY:{item['wal']}")
                physical_rows_for_marker = 0
                for spec in specs:
                    if not isinstance(spec, Mapping):
                        spec = {"relative_path": str(spec or "")}
                    rel_text = str(spec.get("relative_path") or spec.get("path") or "").strip().replace("\\", "/")
                    out = None
                    if rel_text:
                        raw = Path(rel_text)
                        candidate = raw if raw.is_absolute() else (self.base_dir / raw)
                        try:
                            candidate = candidate.resolve()
                            candidate.relative_to(self.parquet_root.resolve())
                            out = candidate
                        except Exception:
                            out = None
                    if out is None:
                        item["errors"].append("PARQUET_OUTPUT_PATH_INVALID")
                        errors.append(f"RESEARCH_PARQUET_OUTPUT_PATH_INVALID:{item['wal']}")
                        continue
                    if not out.is_file() or out.stat().st_size <= 0:
                        item["errors"].append("PARQUET_OUTPUT_MISSING")
                        errors.append(f"RESEARCH_PARQUET_OUTPUT_MISSING:{item['wal']}")
                        continue
                    referenced_outputs.add(out.resolve())
                    output_files += 1
                    actual_sha = _sha256_file(out)
                    _remember_sample_file(out, actual_sha)
                    expected_sha = str(spec.get("file_sha256") or spec.get("sha256") or "").lower()
                    if expected_sha and actual_sha != expected_sha:
                        item["errors"].append("PARQUET_SHA_MISMATCH")
                        errors.append(f"RESEARCH_PARQUET_SHA_MISMATCH:{self._relative_path(out)}")
                    if self._uses_default_parquet_writer:
                        try:
                            import pyarrow.parquet as pq  # type: ignore
                            pf = pq.ParquetFile(out)
                            rows_here = int(pf.metadata.num_rows)
                            physical_rows_for_marker += rows_here
                            metadata = dict(pf.schema_arrow.metadata or {})
                            dataset = (metadata.get(b"ain_dataset") or b"").decode("utf-8", "replace")
                            schema_version = (metadata.get(b"ain_schema_version") or b"").decode("utf-8", "replace")
                            fingerprint = (metadata.get(b"ain_schema_fingerprint") or b"").decode("utf-8", "replace")
                            if not dataset or schema_version != str(SCHEMA_VERSION):
                                item["errors"].append("PARQUET_SCHEMA_METADATA_INVALID")
                                errors.append(f"RESEARCH_PARQUET_SCHEMA_METADATA_INVALID:{self._relative_path(out)}")
                            elif fingerprint != _schema_fingerprint(dataset):
                                item["errors"].append("PARQUET_SCHEMA_FINGERPRINT_MISMATCH")
                                errors.append(f"RESEARCH_PARQUET_SCHEMA_FINGERPRINT_MISMATCH:{self._relative_path(out)}")
                        except Exception as exc:
                            item["errors"].append(f"PARQUET_REOPEN_FAILED:{type(exc).__name__}")
                            errors.append(f"RESEARCH_PARQUET_REOPEN_FAILED:{self._relative_path(out)}:{type(exc).__name__}")
                if self._uses_default_parquet_writer:
                    total_parquet_rows_physical += int(physical_rows_for_marker)
                    if physical_rows_for_marker != marker_rows:
                        item["errors"].append("PARQUET_PHYSICAL_ROW_COUNT_MISMATCH")
                        errors.append(f"RESEARCH_PARQUET_PHYSICAL_ROW_MISMATCH:{item['wal']}")
                if not item["errors"]:
                    markers_valid += 1
            except Exception as exc:
                item["errors"].append(f"VALIDATION_EXCEPTION:{type(exc).__name__}:{exc}")
                errors.append(f"RESEARCH_STORAGE_VALIDATION_EXCEPTION:{item['wal']}:{type(exc).__name__}")
            marker_details.append(item)

        actual_parquet_outputs = {p.resolve() for p in self.parquet_root.rglob("*.parquet")} if self.parquet_root.exists() else set()
        unreferenced_parquet = sorted(str(p) for p in (actual_parquet_outputs - referenced_outputs))
        missing_referenced_parquet = sorted(str(p) for p in (referenced_outputs - actual_parquet_outputs))
        if unreferenced_parquet:
            errors.append("RESEARCH_UNREFERENCED_PARQUET_OUTPUTS_PRESENT")
        if missing_referenced_parquet:
            errors.append("RESEARCH_REFERENCED_PARQUET_OUTPUTS_MISSING")

        sample_entries = sorted(
            sample_entries_by_path.values(), key=lambda item: str(item["path"]).encode("utf-8")
        )
        sample_canonical = json.dumps(
            sample_entries, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8") + b"\n"
        sample_root = {
            "file_count": len(sample_entries),
            "content_root_sha256": hashlib.sha256(sample_canonical).hexdigest(),
            "contract": "A4_2_14_H12H14H3_RESEARCH_SAMPLE_CONTENT_ROOT_V1",
        }
        if int(sample_root.get("file_count", 0) or 0) <= 0:
            errors.append("RESEARCH_SAMPLE_CONTENT_ROOT_EMPTY")

        stats = self._stats_snapshot()
        written = int(stats.get("written_wal", 0) or 0)
        parquet_rows_stat = int(stats.get("parquet_rows", 0) or 0)
        if total_wal_rows != written:
            errors.append("RESEARCH_DISK_WAL_ROWS_VS_RUNTIME_MISMATCH")
        if total_marker_rows != total_wal_rows:
            errors.append("RESEARCH_MARKER_ROWS_VS_WAL_MISMATCH")
        if parquet_rows_stat != total_wal_rows:
            errors.append("RESEARCH_RUNTIME_PARQUET_ROWS_VS_WAL_MISMATCH")
        if self._uses_default_parquet_writer and total_parquet_rows_physical != total_wal_rows:
            errors.append("RESEARCH_PHYSICAL_PARQUET_ROWS_VS_WAL_MISMATCH")
        return {
            "schema_version": 1,
            "contract": "A4_2_14_H12H14H3_RESEARCH_STORAGE_REOPEN_V1",
            "passed": not errors,
            "errors": sorted(set(errors)),
            "wal_files": len(wal_files),
            "open_wal_files": len(open_wal),
            "temp_files": len(tmp_files),
            "valid_conversion_markers": int(markers_valid),
            "parquet_output_files": int(output_files),
            "unreferenced_conversion_markers": len(extra_markers),
            "unreferenced_parquet_outputs": len(unreferenced_parquet),
            "missing_referenced_parquet_outputs": len(missing_referenced_parquet),
            "wal_rows": int(total_wal_rows),
            "recorder_session_id": self.session_id,
            "cohort_mismatch_rows": int(cohort_mismatch_rows),
            "invalid_json_rows": int(invalid_json_rows),
            "marker_parquet_rows": int(total_marker_rows),
            "physical_parquet_rows": int(total_parquet_rows_physical) if self._uses_default_parquet_writer else None,
            "runtime_written_wal": int(written),
            "runtime_parquet_rows": int(parquet_rows_stat),
            "uses_default_pyarrow_writer": bool(self._uses_default_parquet_writer),
            "sample_file_count": int(sample_root.get("file_count", 0) or 0),
            "sample_content_root_sha256": str(sample_root.get("content_root_sha256") or ""),
            "sample_content_root_contract": str(sample_root.get("contract") or ""),
            "details": marker_details,
        }

    def flush(self, timeout: float = 10.0) -> bool:
        deadline = time.time() + max(0.1, float(timeout))
        while (getattr(self._queue, "unfinished_tasks", 0) or getattr(self._critical_queue, "unfinished_tasks", 0)) and time.time() < deadline:
            time.sleep(0.02)
        return not bool(getattr(self._queue, "unfinished_tasks", 0) or getattr(self._critical_queue, "unfinished_tasks", 0))

    def shutdown(self, timeout: float = 20.0) -> bool:
        if not self.enabled:
            return True
        if not self._started:
            with self._lock:
                self._stats["shutdown_complete"] = True
            return True
        self._accepting = False
        self._write_lease(state="shutting_down", force=True)
        deadline = time.time() + max(1.0, float(timeout))
        self._stop.set()
        self._writer_wakeup.set()
        writer = self._writer_thread
        if writer is not None and writer.is_alive() and writer is not threading.current_thread():
            writer.join(timeout=max(0.1, deadline - time.time()))
        writer_complete = not (writer is not None and writer.is_alive())
        self._converter_stop.set()
        converter = self._converter_thread
        if converter is not None and converter.is_alive() and converter is not threading.current_thread():
            converter.join(timeout=max(0.1, deadline - time.time()))
        converter_complete = not (converter is not None and converter.is_alive())
        with self._lock:
            self._stats["shutdown_complete"] = bool(
                writer_complete and converter_complete and self._queue.empty() and self._critical_queue.empty() and not self._durability_pending
            )
        if self._stats["shutdown_complete"]:
            self._remove_lease()
        self._write_stats()
        return bool(self._stats["shutdown_complete"])


_DEFAULT_LOCK = threading.RLock()
_DEFAULT: Optional[FullSourceObservationRecorder] = None


def _default() -> FullSourceObservationRecorder:
    global _DEFAULT
    if _DEFAULT is None:
        with _DEFAULT_LOCK:
            if _DEFAULT is None:
                _DEFAULT = FullSourceObservationRecorder()
    return _DEFAULT


def enabled() -> bool:
    return _default().is_enabled()


def record_source_observation_receipt(
    *, market_key: str, observation: Mapping[str, Any],
    runtime_context: Optional[Mapping[str, Any]] = None,
    duplicate_is_success: bool = False, require_durable: Optional[bool] = None,
    durable_timeout: Optional[float] = None,
) -> Dict[str, Any]:
    return _default().record_source_observation_receipt(
        market_key=market_key, observation=observation,
        runtime_context=runtime_context, duplicate_is_success=bool(duplicate_is_success),
        require_durable=require_durable, durable_timeout=durable_timeout,
    )

def record_source_observation(
    *, market_key: str, observation: Mapping[str, Any],
    runtime_context: Optional[Mapping[str, Any]] = None,
    duplicate_is_success: bool = False, require_durable: Optional[bool] = None,
    durable_timeout: Optional[float] = None,
) -> bool:
    return _default().record_source_observation(
        market_key=market_key, observation=observation,
        runtime_context=runtime_context, duplicate_is_success=bool(duplicate_is_success),
        require_durable=require_durable, durable_timeout=durable_timeout,
    )


def record_decision_enrichment(*, market_key: str, observation: Mapping[str, Any], probability: Optional[Mapping[str, Any]] = None, paac_row: Optional[Mapping[str, Any]] = None, live_row: Optional[Mapping[str, Any]] = None, runtime_context: Optional[Mapping[str, Any]] = None, event_type: str = "DECISION_ENRICHMENT") -> bool:
    return _default().record_decision_enrichment(
        market_key=market_key, observation=observation, probability=probability,
        paac_row=paac_row, live_row=live_row, runtime_context=runtime_context,
        event_type=event_type,
    )


def record_decision_context_snapshot(*, market_key: str, observation: Mapping[str, Any], probability: Optional[Mapping[str, Any]] = None, live_row: Optional[Mapping[str, Any]] = None, runtime_context: Optional[Mapping[str, Any]] = None, active_rows: Optional[Iterable[Mapping[str, Any]]] = None, event_type: str = "DECISION_CONTEXT_SNAPSHOT") -> bool:
    return _default().record_decision_context_snapshot(
        market_key=market_key, observation=observation, probability=probability, live_row=live_row,
        runtime_context=runtime_context, active_rows=active_rows, event_type=event_type,
    )


def record_event_update(*, market_key: str, observation: Mapping[str, Any], event_row: Mapping[str, Any], event_type: str) -> bool:
    return _default().record_event_update(market_key=market_key, observation=observation, event_row=event_row, event_type=event_type)


def validate_frozen_sample_storage() -> Dict[str, Any]:
    return _default().validate_frozen_sample_storage()


def stats() -> Dict[str, Any]:
    return _default().stats()


def shutdown(timeout: float = 20.0) -> bool:
    return _default().shutdown(timeout=timeout)


def compact_closed_wal(base_dir: Optional[Path] = None, *, recover_open: bool = False) -> Dict[str, Any]:
    """Compact WAL. --recover-open recovers only files whose owner is confirmed dead.

    Files owned by a live recorder are never renamed. Unknown-owner files are
    deferred until they exceed the configured grace period.
    """
    recorder = FullSourceObservationRecorder(base_dir=base_dir, enabled=True, start_immediately=False)
    recorder.base_dir.mkdir(parents=True, exist_ok=True)
    recorder.wal_root.mkdir(parents=True, exist_ok=True)
    recorder.parquet_root.mkdir(parents=True, exist_ok=True)
    recorder.stats_root.mkdir(parents=True, exist_ok=True)
    recorder.lock_root.mkdir(parents=True, exist_ok=True)
    recovery = recorder._recover_orphan_segments() if recover_open else {"recovered": 0, "active_skipped": 0, "unknown_deferred": 0}
    converted = failed = 0
    for path in recorder.wal_root.rglob("*.jsonl.closed"):
        if recorder._convert_one(path):
            converted += 1
        else:
            failed += 1
    payload = recorder.stats()
    payload.update({
        "converted_segments": converted,
        "failed_segments": failed,
        "recovered_open_segments": recovery["recovered"],
        "active_open_segments_skipped": recovery["active_skipped"],
        "unknown_open_segments_deferred": recovery["unknown_deferred"],
    })
    recorder._write_stats()
    return payload


def _shutdown_default_at_exit() -> None:
    # Do not instantiate the recorder merely because Python is exiting.
    recorder = _DEFAULT
    if recorder is not None:
        try:
            recorder.shutdown(timeout=15.0)
        except Exception as exc:
            record_stage_error(
                "wal_fsync", "recorder_atexit_shutdown", exc,
                reason_code="RECORDER_ATEXIT_SHUTDOWN_FAILED", recoverable=False,
            )


atexit.register(_shutdown_default_at_exit)

__all__ = [
    "VERSION", "SCHEMA_VERSION", "FullSourceObservationRecorder",
    "enabled", "record_source_observation", "record_decision_enrichment",
    "record_event_update", "record_decision_context_snapshot", "validate_frozen_sample_storage", "stats", "shutdown", "compact_closed_wal",
    "parquet_engine_available", "parquet_engine_name", "normalize_row_for_dataset",
]
