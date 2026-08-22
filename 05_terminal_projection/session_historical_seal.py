# -*- coding: utf-8 -*-
"""A112 Session-Scoped Historical Seal.

This module is evidence-only.  It does not score signals, select symbols, change
thresholds, or authorize live decisions.  Its job is narrower and deliberately
boring:

* accept every lifecycle evidence operation durably before writer execution;
* allocate ordering inside SQLite, never in caller memory;
* rotate session epochs atomically;
* keep independent, short Live and Shadow writer barriers;
* persist canonical historical checkpoints and append-only attempts;
* write one crash-safe sidecar per closed epoch;
* verify old sessions from their stored historical material, not mutable tables.

The implementation is safe to import when the A112 shadow feature is disabled.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import hashlib
import json
import math
import os
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from exception_observability import report_suppressed_exception as _report_suppressed_exception


SEAL_SCHEMA_VERSION = "A112_SESSION_SEAL_V3"
CANONICALIZATION_VERSION = "STRICT_JSON_V2"
FIELD_POLICY_VERSION = "LIFECYCLE_EVIDENCE_V3"
COMPARISON_CONTRACT_VERSION = "LIVE_SHADOW_HISTORICAL_V3"
APPLICATION_BUILD_ID = "A112_SESSION_SCOPED_HISTORICAL_SEAL_LIFECYCLE_ATOMIC_TRUTH_HARDENED_CANDIDATE"

OPEN = "OPEN"
DRAINING = "DRAINING"
BUNDLE_PREPARED = "BUNDLE_PREPARED"
SIDECAR_COMMITTED = "SIDECAR_COMMITTED"
SEALED_VALID = "SEALED_VALID"
SEALED_INVALID = "SEALED_INVALID"

PENDING = "PENDING"
TERMINAL_SUCCESS = "TERMINAL_SUCCESS"
TERMINAL_FAILURE = "TERMINAL_FAILURE"
TERMINAL_STATUSES = frozenset({TERMINAL_SUCCESS, TERMINAL_FAILURE})
FINAL_SEAL_STATUSES = frozenset({SEALED_VALID, SEALED_INVALID})
_TERMINAL_LIFECYCLE_STATES = frozenset({"TARGET_HIT", "STOP_HIT", "TIMEOUT", "ABORTED", "EOD_SQUARE_OFF", "DELETED"})
_TERMINAL_SUCCESS_RESULTS = frozenset({
    "APPLIED", "SHADOW_APPLIED", "SQLITE_COMMITTED", "DERIVED_EVENT",
    "NOOP_IDENTICAL", "ACKED", "COMMITTED",
})
_TERMINAL_FAILURE_RESULTS = frozenset({
    "SHADOW_REJECTED", "PROCESS_STOPPED", "TIMEOUT", "CANCELLED", "ABORTED",
    "DROPPED", "SKIPPED", "REPLAY_DECODE_FAILURE", "BARRIER_REQUEUE_FAILURE",
    "SHADOW_PROCESSING_FAILURE", "REJECTED", "FAILED", "ERROR",
})
_COMPARISON_COUNT_KEYS = (
    "financial_mismatches", "event_mismatches", "orphan_observations",
    "mirror_drops", "processing_failures", "unfinished_ingress",
    "cross_session_leakage",
)
_FINANCIAL_TRUTH_FIELDS = (
    # Identity and legal decision context.  These fields are not cosmetic: a
    # checkpoint for another episode, market, side or bar is not equivalent.
    "episode_id", "episode_key", "symbol", "market_key", "decision_lane",
    "timeframe", "side", "signal_bar", "signal_time", "signal_bar_is_sealed",
    "state", "state_ar", "entry_status", "last_event", "source",
    # Frozen acceptance/execution truth.
    "entry_price", "current_price", "exit_price", "stop_price", "target_price",
    "original_stop", "original_target", "gann_evaluation_price",
    "acceptance_anchor_price", "acceptance_evaluation_price",
    "acceptance_breakout_price", "acceptance_target50_price",
    "acceptance_stop_reference", "execution_anchor_price",
    "execution_stop_price", "execution_target50_price",
    "execution_fast_silver_price",
    # Outcome/ranking truth.
    "pnl_pct", "mfe_pct", "mae_pct", "progress_ratio", "rank_strength",
    "time_horizon", "bars_elapsed", "max_bars", "extension_count",
    "upgrade_count", "observed_bar_count", "last_observed_bar",
    "radar_stage", "terminal", "deleted",
)
_EVENT_TRUTH_FIELDS = (
    "operation_id", "logical_event_id", "causal_occurrence_id",
    "event_type", "state_before", "state_after", "price", "exit_price",
    "pnl_pct", "reason_code", "entry_status", "episode_id", "episode_key",
    "symbol", "market_key", "timeframe", "side",
)
_INGRESS_IMMUTABLE_TRUTH_FIELDS = (
    "episode_id", "episode_key", "symbol", "market_key", "decision_lane",
    "timeframe", "side", "signal_bar", "signal_time", "signal_bar_is_sealed",
    "entry_price", "stop_price", "target_price", "original_stop",
    "original_target", "gann_evaluation_price", "execution_anchor_price",
    "execution_stop_price", "execution_target50_price",
)

SIDES = frozenset({"LIVE", "SHADOW"})
KINDS = frozenset({"upsert", "event", "transition", "delete", "observation"})

_BOOL_FIELDS = frozenset({
    "signal_bar_is_sealed", "terminal", "deleted", "session_close",
    "is_official", "is_active", "is_saved", "is_valid",
})
_NUMERIC_SUFFIXES = (
    "_price", "_pct", "_ratio", "_count", "_revision", "_sequence",
    "_bars", "_strength", "_probability",
)
_NUMERIC_FIELDS = frozenset({
    "entry_price", "stop_price", "target_price", "current_price", "exit_price",
    "rank_strength", "bars_elapsed", "max_bars", "extension_count",
    "upgrade_count", "observed_bar_count", "pnl_pct", "mfe_pct", "mae_pct",
})
_REQUIRED_FINITE_FIELDS = frozenset({
    "entry_price", "stop_price", "target_price", "current_price", "exit_price",
    "acceptance_anchor_price", "execution_anchor_price",
})
_VOLATILE_KEYS = frozenset({
    "_lifecycle_retry_count", "done_event", "result_box",
})

_TIME_TRUTH_FIELDS = frozenset({
    "signal_bar", "signal_time", "entry_bar", "event_time",
    "episode_signal_bar_time", "acceptance_anchor_time",
})


def _normalize_time_truth(value: Any, *, field: str) -> Any:
    if value in (None, ""):
        return value
    text = str(value).strip()
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        try:
            parsed_time = dt.time.fromisoformat(text.replace("Z", "+00:00"))
            return parsed_time.replace(tzinfo=None).isoformat(timespec="milliseconds")
        except Exception as exc:
            raise HistoricalSealError(f"INVALID_TIME:{field}") from exc
    # The lifecycle contract stores market-local wall-clock bars.  Shadow may
    # remove the explicit offset while preserving that wall time.
    return parsed.replace(tzinfo=None).isoformat(timespec="milliseconds")


class HistoricalSealError(RuntimeError):
    """Base error for the historical seal."""


class ProducerFenceError(HistoricalSealError):
    """A stale process attempted to accept evidence."""


class OperationPayloadMismatch(HistoricalSealError):
    """The same operation id was reused with different canonical content."""


class HistoricalBarrierBlocked(HistoricalSealError):
    """A writer attempted a new epoch before its old checkpoint existed."""


class CheckpointNotReady(HistoricalSealError):
    """At least one accepted operation has no terminal receipt for this side."""


class HistoricalVerificationError(HistoricalSealError):
    """Stored historical material failed self-verification."""


@dataclasses.dataclass(frozen=True)
class ProducerIdentity:
    decision_lane: str
    generation: int
    instance_id: str
    claimed_at: str


@dataclasses.dataclass(frozen=True)
class IngressReceipt:
    accept_sequence: int
    evidence_scope: str
    epoch_token: str
    operation_id: str
    canonical_payload_hash: str
    accepted_at: str
    producer_generation: int
    producer_instance_id: str
    idempotent_replay: bool = False

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass(frozen=True)
class RotationReceipt:
    evidence_scope: str
    closed_epoch_token: str
    new_epoch_token: str
    cutoff_sequence: int
    rotated_at: str
    session_id: str = ""
    market_session_date: str = ""
    session_identity_hash: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)


def utc_now_text() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: str) -> str:
    return _sha256_bytes(value.encode("utf-8"))


def _normalize_bool(value: Any, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1"}:
            return True
        if text in {"false", "0"}:
            return False
    raise HistoricalSealError(f"INVALID_BOOLEAN:{field}")


def _strict_value(value: Any, *, field: str = "") -> Any:
    if field in _TIME_TRUTH_FIELDS and value not in (None, ""):
        return _normalize_time_truth(value, field=field)
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HistoricalSealError(f"NON_FINITE_NUMBER:{field or 'value'}")
        return int(value) if value.is_integer() else value
    if isinstance(value, (dt.datetime, dt.date, dt.time, Path)):
        return str(value)
    if isinstance(value, Mapping):
        clean: Dict[str, Any] = {}
        for key, child in value.items():
            name = str(key)
            if name in _VOLATILE_KEYS:
                continue
            if name in _BOOL_FIELDS and child is not None:
                clean[name] = _normalize_bool(child, field=name)
                continue
            if child is not None and (name in _NUMERIC_FIELDS or name.endswith(_NUMERIC_SUFFIXES)):
                if isinstance(child, bool):
                    raise HistoricalSealError(f"BOOLEAN_AS_NUMBER:{name}")
                if isinstance(child, int):
                    clean[name] = child
                    continue
                if isinstance(child, float):
                    if not math.isfinite(child):
                        if name in _REQUIRED_FINITE_FIELDS:
                            raise HistoricalSealError(f"NON_FINITE_NUMBER:{name}")
                        clean[name] = None
                    else:
                        clean[name] = int(child) if child.is_integer() else child
                    continue
                try:
                    raw = str(child).strip()
                    if re.fullmatch(r"[+-]?\d+", raw):
                        clean[name] = int(raw)
                        continue
                    number = float(raw)
                except Exception as exc:
                    raise HistoricalSealError(f"INVALID_NUMBER:{name}") from exc
                if not math.isfinite(number):
                    if name in _REQUIRED_FINITE_FIELDS:
                        raise HistoricalSealError(f"NON_FINITE_NUMBER:{name}")
                    clean[name] = None
                    continue
                clean[name] = int(number) if number.is_integer() else number
                continue
            clean[name] = _strict_value(child, field=name)
        return clean
    if isinstance(value, (set, frozenset)):
        raise HistoricalSealError(f"UNORDERED_COLLECTION_NOT_CANONICAL:{field or 'value'}")
    if isinstance(value, (list, tuple)):
        return [_strict_value(child, field=field) for child in value]
    text = str(value)
    if re.search(r"0x[0-9a-fA-F]+", text):
        raise HistoricalSealError(f"NON_CANONICAL_OBJECT:{field or type(value).__name__}")
    return text


def canonical_json_bytes(value: Any) -> bytes:
    clean = _strict_value(value)
    text = json.dumps(
        clean, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    )
    return text.encode("utf-8")


def canonical_json_text(value: Any) -> str:
    return canonical_json_bytes(value).decode("utf-8")


def normalize_market_key(value: Any) -> str:
    text = str(value or "").strip()
    return text if text else "UNKNOWN_MARKET"


def evidence_scope_for(decision_lane: str, payload: Mapping[str, Any]) -> str:
    lane = str(decision_lane or "model").strip().lower()
    market = normalize_market_key(
        payload.get("market_key") or payload.get("market") or payload.get("market_id")
    )
    return f"{lane}::{market}"


def session_identity_for_payload(payload: Mapping[str, Any]) -> Dict[str, str]:
    raw = dict(payload or {})
    market = normalize_market_key(raw.get("market_key") or raw.get("market") or raw.get("market_id"))
    session_date = ""
    for key in ("market_session_date", "session_date", "signal_bar", "bar_key",
                "recommendation_datetime", "signal_time", "event_time", "created_at", "updated_at"):
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", str(raw.get(key) or ""))
        if match:
            session_date = match.group(1)
            break
    if not session_date:
        raise HistoricalSealError("MARKET_SESSION_DATE_REQUIRED_AT_ACCEPT")
    explicit_session_id = str(raw.get("session_id") or raw.get("market_session_id") or "").strip()
    if explicit_session_id:
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", explicit_session_id)
        if match and match.group(1) != session_date:
            raise HistoricalSealError("SESSION_ID_DATE_MISMATCH")
    session_id = explicit_session_id or f"{market}:{session_date}"
    evidence_session_id = str(raw.get("evidence_session_id") or session_id).strip()
    session_scope_key = f"{market}:{session_date}"
    return {
        "market_key": market,
        "market_session_date": session_date,
        "market_session_id": session_id,
        "evidence_session_id": evidence_session_id,
        "session_scope_key": session_scope_key,
    }


def operation_id_for(kind: str, payload: Mapping[str, Any], *, decision_lane: str) -> str:
    kind_text = str(kind or "").strip().lower()
    explicit = str(
        payload.get("_a112_operation_id")
        or payload.get("_lifecycle_outbox_id")
        or payload.get("logical_event_id")
        or payload.get("causal_occurrence_id")
        or payload.get("source_event_id")
        or payload.get("transition_event_id")
        or payload.get("observation_occurrence_id")
        or ""
    ).strip()
    if explicit:
        return explicit
    if kind_text == "observation":
        raise HistoricalSealError("OBSERVATION_OCCURRENCE_ID_REQUIRED")
    stable_payload = dict(payload or {})
    for key in list(stable_payload):
        if str(key).startswith("_a112_") or key in _VOLATILE_KEYS:
            stable_payload.pop(key, None)
    envelope = {
        "decision_lane": str(decision_lane or "").strip().lower(),
        "kind": kind_text,
        "signal_id": str(payload.get("id") or payload.get("signal_id") or "").strip(),
        "payload": stable_payload,
    }
    return "A112OP-" + _sha256_bytes(canonical_json_bytes(envelope))


def historical_db_for_live_db(live_db_path: str) -> str:
    path = Path(str(live_db_path)).resolve()
    return str(path.with_name(path.name + ".a112_shadow.sqlite"))


def env_historical_seal_enabled() -> bool:
    return str(os.getenv("AIN_A112_SURGICAL_SHADOW", "0")).strip().lower() in {
        "1", "true", "yes", "on",
    }


_SCHEMA = """
BEGIN IMMEDIATE;
CREATE TABLE IF NOT EXISTS evidence_producers (
    decision_lane TEXT PRIMARY KEY,
    generation INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    claimed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence_current_epochs (
    evidence_scope TEXT PRIMARY KEY,
    decision_lane TEXT NOT NULL,
    epoch_token TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS evidence_epochs (
    evidence_scope TEXT NOT NULL,
    epoch_token TEXT NOT NULL,
    decision_lane TEXT NOT NULL,
    status TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    cutoff_sequence INTEGER,
    producer_generation INTEGER NOT NULL,
    producer_instance_id TEXT NOT NULL,
    session_details_blob BLOB,
    session_details_hash TEXT,
    session_id TEXT,
    market_session_date TEXT,
    session_identity_hash TEXT,
    market_key TEXT,
    session_scope_key TEXT,
    evidence_session_id TEXT,
    PRIMARY KEY (evidence_scope, epoch_token)
);
CREATE INDEX IF NOT EXISTS idx_evidence_epochs_status
    ON evidence_epochs(status, evidence_scope, opened_at);
CREATE TABLE IF NOT EXISTS evidence_ingress (
    accept_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    evidence_scope TEXT NOT NULL,
    epoch_token TEXT NOT NULL,
    operation_id TEXT NOT NULL UNIQUE,
    canonical_payload_blob BLOB NOT NULL,
    canonical_payload_hash TEXT NOT NULL,
    kind TEXT NOT NULL,
    signal_id TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    producer_generation INTEGER NOT NULL,
    producer_instance_id TEXT NOT NULL,
    market_key TEXT,
    market_session_date TEXT,
    session_scope_key TEXT,
    evidence_session_id TEXT,
    live_status TEXT NOT NULL DEFAULT 'PENDING',
    shadow_status TEXT NOT NULL DEFAULT 'PENDING',
    live_result_blob BLOB,
    live_result_hash TEXT,
    shadow_result_blob BLOB,
    shadow_result_hash TEXT,
    FOREIGN KEY (evidence_scope, epoch_token)
        REFERENCES evidence_epochs(evidence_scope, epoch_token)
);
CREATE INDEX IF NOT EXISTS idx_evidence_ingress_epoch
    ON evidence_ingress(evidence_scope, epoch_token, accept_sequence);
CREATE INDEX IF NOT EXISTS idx_evidence_ingress_live
    ON evidence_ingress(evidence_scope, epoch_token, live_status, accept_sequence);
CREATE INDEX IF NOT EXISTS idx_evidence_ingress_shadow
    ON evidence_ingress(evidence_scope, epoch_token, shadow_status, accept_sequence);
CREATE TABLE IF NOT EXISTS evidence_execution_attempts (
    attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
    accept_sequence INTEGER NOT NULL,
    side TEXT NOT NULL,
    attempt_number INTEGER NOT NULL,
    result TEXT NOT NULL,
    terminal INTEGER NOT NULL DEFAULT 0,
    terminal_status TEXT NOT NULL DEFAULT 'PENDING',
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL,
    result_blob BLOB,
    result_hash TEXT,
    error_hash TEXT,
    UNIQUE(accept_sequence, side, attempt_number),
    FOREIGN KEY (accept_sequence) REFERENCES evidence_ingress(accept_sequence)
);
CREATE INDEX IF NOT EXISTS idx_evidence_attempts_sequence
    ON evidence_execution_attempts(accept_sequence, side, attempt_number);
CREATE TABLE IF NOT EXISTS evidence_combined_session_verdicts (
    session_key TEXT NOT NULL,
    decision_lane TEXT NOT NULL,
    market_key TEXT NOT NULL,
    market_session_date TEXT NOT NULL,
    evidence_session_id TEXT NOT NULL,
    producer_generation INTEGER NOT NULL,
    producer_instance_id TEXT NOT NULL,
    shadow_seal_hash TEXT NOT NULL,
    historical_epoch_ids_blob BLOB NOT NULL,
    historical_epoch_seal_hashes_blob BLOB NOT NULL,
    combined_valid INTEGER NOT NULL,
    combined_reason TEXT,
    combined_payload_blob BLOB NOT NULL,
    combined_payload_hash TEXT NOT NULL,
    finalized_at TEXT NOT NULL,
    PRIMARY KEY (decision_lane, session_key)
);
CREATE TABLE IF NOT EXISTS evidence_seals (
    evidence_scope TEXT NOT NULL,
    epoch_token TEXT NOT NULL,
    cutoff_sequence INTEGER NOT NULL,
    status TEXT NOT NULL,
    session_id TEXT,
    market_session_date TEXT,
    session_identity_hash TEXT,
    session_details_hash TEXT,
    live_checkpoint_blob BLOB,
    live_checkpoint_hash TEXT,
    shadow_checkpoint_blob BLOB,
    shadow_checkpoint_hash TEXT,
    comparison_blob BLOB,
    comparison_hash TEXT,
    ingress_hash TEXT,
    attempts_hash TEXT,
    seal_bundle_blob BLOB,
    seal_bundle_hash TEXT,
    sidecar_path TEXT,
    invalid_reason TEXT,
    prepared_at TEXT,
    sealed_at TEXT,
    PRIMARY KEY (evidence_scope, epoch_token),
    FOREIGN KEY (evidence_scope, epoch_token)
        REFERENCES evidence_epochs(evidence_scope, epoch_token)
);
COMMIT;
"""

_TRIGGER_SQL = """
CREATE TRIGGER IF NOT EXISTS trg_a112_combined_verdict_update_immutable
BEFORE UPDATE ON evidence_combined_session_verdicts
BEGIN
    SELECT RAISE(ABORT, 'COMBINED_VERDICT_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_combined_verdict_delete_immutable
BEFORE DELETE ON evidence_combined_session_verdicts
BEGIN
    SELECT RAISE(ABORT, 'COMBINED_VERDICT_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_seal_immutable
BEFORE UPDATE ON evidence_seals
WHEN OLD.status IN ('SEALED_VALID','SEALED_INVALID')
BEGIN
    SELECT RAISE(ABORT, 'FINAL_SEAL_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_epoch_immutable
BEFORE UPDATE ON evidence_epochs
WHEN OLD.status IN ('SEALED_VALID','SEALED_INVALID')
BEGIN
    SELECT RAISE(ABORT, 'FINAL_EPOCH_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_ingress_update_immutable
BEFORE UPDATE ON evidence_ingress
WHEN EXISTS (
    SELECT 1 FROM evidence_epochs e
    WHERE e.evidence_scope=OLD.evidence_scope AND e.epoch_token=OLD.epoch_token
      AND e.status IN ('SEALED_VALID','SEALED_INVALID')
)
BEGIN
    SELECT RAISE(ABORT, 'FINAL_INGRESS_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_ingress_delete_immutable
BEFORE DELETE ON evidence_ingress
WHEN EXISTS (
    SELECT 1 FROM evidence_epochs e
    WHERE e.evidence_scope=OLD.evidence_scope AND e.epoch_token=OLD.epoch_token
      AND e.status IN ('SEALED_VALID','SEALED_INVALID')
)
BEGIN
    SELECT RAISE(ABORT, 'FINAL_INGRESS_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_attempt_insert_immutable
BEFORE INSERT ON evidence_execution_attempts
WHEN EXISTS (
    SELECT 1 FROM evidence_ingress i JOIN evidence_epochs e
      ON e.evidence_scope=i.evidence_scope AND e.epoch_token=i.epoch_token
    WHERE i.accept_sequence=NEW.accept_sequence
      AND e.status IN ('SEALED_VALID','SEALED_INVALID')
)
BEGIN
    SELECT RAISE(ABORT, 'FINAL_ATTEMPT_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_attempt_update_immutable
BEFORE UPDATE ON evidence_execution_attempts
WHEN EXISTS (
    SELECT 1 FROM evidence_ingress i JOIN evidence_epochs e
      ON e.evidence_scope=i.evidence_scope AND e.epoch_token=i.epoch_token
    WHERE i.accept_sequence=OLD.accept_sequence
      AND e.status IN ('SEALED_VALID','SEALED_INVALID')
)
BEGIN
    SELECT RAISE(ABORT, 'FINAL_ATTEMPT_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_attempt_delete_immutable
BEFORE DELETE ON evidence_execution_attempts
WHEN EXISTS (
    SELECT 1 FROM evidence_ingress i JOIN evidence_epochs e
      ON e.evidence_scope=i.evidence_scope AND e.epoch_token=i.epoch_token
    WHERE i.accept_sequence=OLD.accept_sequence
      AND e.status IN ('SEALED_VALID','SEALED_INVALID')
)
BEGIN
    SELECT RAISE(ABORT, 'FINAL_ATTEMPT_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_seal_delete_immutable
BEFORE DELETE ON evidence_seals
WHEN OLD.status IN ('SEALED_VALID','SEALED_INVALID')
BEGIN
    SELECT RAISE(ABORT, 'FINAL_SEAL_IMMUTABLE');
END;
CREATE TRIGGER IF NOT EXISTS trg_a112_final_epoch_delete_immutable
BEFORE DELETE ON evidence_epochs
WHEN OLD.status IN ('SEALED_VALID','SEALED_INVALID')
BEGIN
    SELECT RAISE(ABORT, 'FINAL_EPOCH_IMMUTABLE');
END;
"""



class SessionHistoricalSeal:
    """Durable, session-scoped historical evidence coordinator."""

    def __init__(
        self,
        db_path: str,
        *,
        decision_lane: str,
        sidecar_dir: Optional[str] = None,
        application_build_id: str = APPLICATION_BUILD_ID,
        busy_timeout_ms: int = 8000,
    ) -> None:
        self.db_path = str(Path(db_path).resolve())
        self.decision_lane = str(decision_lane or "model").strip().lower()
        if self.decision_lane not in {"model", "radar"}:
            raise HistoricalSealError("INVALID_DECISION_LANE")
        self.sidecar_dir = Path(
            sidecar_dir or (str(Path(self.db_path).with_suffix(Path(self.db_path).suffix + ".seals")))
        ).resolve()
        self.application_build_id = str(application_build_id or APPLICATION_BUILD_ID)
        self.busy_timeout_ms = max(1000, int(busy_timeout_ms))
        self._lock = threading.RLock()
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.sidecar_dir.mkdir(parents=True, exist_ok=True)
        self._init_schema()
        self.identity = self._claim_producer_identity()

    def _connect(self, *, busy_timeout_ms: Optional[int] = None) -> sqlite3.Connection:
        timeout_ms = self.busy_timeout_ms if busy_timeout_ms is None else max(0, int(busy_timeout_ms))
        conn = sqlite3.connect(
            self.db_path,
            timeout=max(0.0, timeout_ms / 1000.0),
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={timeout_ms}")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @staticmethod
    def _migrate_combined_verdict_lane_key(conn: sqlite3.Connection) -> None:
        """Upgrade the immutable verdict key from session-only to lane+session."""
        info = conn.execute("PRAGMA table_info(evidence_combined_session_verdicts)").fetchall()
        if not info:
            return
        primary = [str(row[1]) for row in sorted((row for row in info if int(row[5] or 0) > 0), key=lambda row: int(row[5]))]
        if primary == ["decision_lane", "session_key"]:
            return
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("DROP TRIGGER IF EXISTS trg_a112_combined_verdict_update_immutable")
            conn.execute("DROP TRIGGER IF EXISTS trg_a112_combined_verdict_delete_immutable")
            conn.execute("ALTER TABLE evidence_combined_session_verdicts RENAME TO evidence_combined_session_verdicts_legacy")
            conn.execute(
                """
                CREATE TABLE evidence_combined_session_verdicts (
                    session_key TEXT NOT NULL, decision_lane TEXT NOT NULL,
                    market_key TEXT NOT NULL, market_session_date TEXT NOT NULL,
                    evidence_session_id TEXT NOT NULL, producer_generation INTEGER NOT NULL,
                    producer_instance_id TEXT NOT NULL, shadow_seal_hash TEXT NOT NULL,
                    historical_epoch_ids_blob BLOB NOT NULL,
                    historical_epoch_seal_hashes_blob BLOB NOT NULL,
                    combined_valid INTEGER NOT NULL, combined_reason TEXT,
                    combined_payload_blob BLOB NOT NULL, combined_payload_hash TEXT NOT NULL,
                    finalized_at TEXT NOT NULL, PRIMARY KEY (decision_lane, session_key)
                )
                """
            )
            conn.execute(
                """
                INSERT INTO evidence_combined_session_verdicts(
                    session_key,decision_lane,market_key,market_session_date,evidence_session_id,
                    producer_generation,producer_instance_id,shadow_seal_hash,
                    historical_epoch_ids_blob,historical_epoch_seal_hashes_blob,
                    combined_valid,combined_reason,combined_payload_blob,combined_payload_hash,finalized_at
                )
                SELECT session_key,decision_lane,market_key,market_session_date,evidence_session_id,
                       producer_generation,producer_instance_id,shadow_seal_hash,
                       historical_epoch_ids_blob,historical_epoch_seal_hashes_blob,
                       combined_valid,combined_reason,combined_payload_blob,combined_payload_hash,finalized_at
                FROM evidence_combined_session_verdicts_legacy
                """
            )
            conn.execute("DROP TABLE evidence_combined_session_verdicts_legacy")
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    def _init_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
            migrations = {
                "evidence_epochs": {
                    "session_details_hash": "TEXT", "session_id": "TEXT",
                    "market_session_date": "TEXT", "session_identity_hash": "TEXT",
                    "market_key": "TEXT", "session_scope_key": "TEXT",
                    "evidence_session_id": "TEXT",
                },
                "evidence_seals": {
                    "session_id": "TEXT", "market_session_date": "TEXT",
                    "session_identity_hash": "TEXT", "session_details_hash": "TEXT",
                },
                "evidence_execution_attempts": {"terminal_status": "TEXT NOT NULL DEFAULT 'PENDING'"},
                "evidence_ingress": {"market_key": "TEXT", "market_session_date": "TEXT",
                    "session_scope_key": "TEXT", "evidence_session_id": "TEXT"},
            }
            for table, columns in migrations.items():
                existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
                for name, ddl in columns.items():
                    if name not in existing:
                        conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")
            self._migrate_combined_verdict_lane_key(conn)
            conn.executescript(_TRIGGER_SQL)
        finally:
            conn.close()

    def _claim_producer_identity(self) -> ProducerIdentity:
        conn = self._connect()
        now = utc_now_text()
        instance_id = f"{os.getpid()}:{time.time_ns()}:{uuid.uuid4().hex}"
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT generation FROM evidence_producers WHERE decision_lane=?",
                (self.decision_lane,),
            ).fetchone()
            generation = int(row["generation"] or 0) + 1 if row else 1
            conn.execute(
                """
                INSERT INTO evidence_producers(decision_lane,generation,instance_id,claimed_at)
                VALUES(?,?,?,?)
                ON CONFLICT(decision_lane) DO UPDATE SET
                    generation=excluded.generation,
                    instance_id=excluded.instance_id,
                    claimed_at=excluded.claimed_at
                """,
                (self.decision_lane, generation, instance_id, now),
            )
            # A process restart may legitimately continue an already-open epoch.
            # Rebind only OPEN epochs. Closed epochs retain their historical owner.
            conn.execute(
                """
                UPDATE evidence_epochs
                SET producer_generation=?, producer_instance_id=?
                WHERE decision_lane=? AND status IN ('OPEN','DRAINING','BUNDLE_PREPARED','SIDECAR_COMMITTED')
                """,
                (generation, instance_id, self.decision_lane),
            )
            conn.execute("COMMIT")
            return ProducerIdentity(self.decision_lane, generation, instance_id, now)
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def current_process_identity(self) -> Dict[str, Any]:
        return dataclasses.asdict(self.identity)

    def operation_receipt(self, operation_id: str) -> Optional[IngressReceipt]:
        op_id = str(operation_id or "").strip()
        if not op_id:
            return None
        conn = self._connect(busy_timeout_ms=500)
        try:
            row = conn.execute(
                "SELECT * FROM evidence_ingress WHERE operation_id=?", (op_id,)
            ).fetchone()
            if row is None:
                return None
            return IngressReceipt(
                accept_sequence=int(row["accept_sequence"]),
                evidence_scope=str(row["evidence_scope"]),
                epoch_token=str(row["epoch_token"]),
                operation_id=str(row["operation_id"]),
                canonical_payload_hash=str(row["canonical_payload_hash"]),
                accepted_at=str(row["accepted_at"]),
                producer_generation=int(row["producer_generation"]),
                producer_instance_id=str(row["producer_instance_id"]),
                idempotent_replay=True,
            )
        finally:
            conn.close()

    @staticmethod
    def _decode_combined_payload(row: sqlite3.Row) -> Dict[str, Any]:
        blob = bytes(row["combined_payload_blob"] or b"")
        expected = str(row["combined_payload_hash"] or "")
        if not blob or _sha256_bytes(blob) != expected:
            raise HistoricalVerificationError("COMBINED_VERDICT_HASH_MISMATCH")
        try:
            payload = json.loads(blob.decode("utf-8"))
        except Exception as exc:
            raise HistoricalVerificationError("COMBINED_VERDICT_JSON_INVALID") from exc
        if not isinstance(payload, dict):
            raise HistoricalVerificationError("COMBINED_VERDICT_MAPPING_REQUIRED")
        if bool(payload.get("sealed_valid")) != bool(int(row["combined_valid"] or 0)):
            raise HistoricalVerificationError("COMBINED_VERDICT_VALIDITY_MISMATCH")
        if str(payload.get("session_key") or "") != str(row["session_key"] or ""):
            raise HistoricalVerificationError("COMBINED_VERDICT_SESSION_MISMATCH")
        if str(payload.get("decision_lane") or "").lower() != str(row["decision_lane"] or "").lower():
            raise HistoricalVerificationError("COMBINED_VERDICT_LANE_MISMATCH")
        return {**payload, "combined_result_hash": expected}

    def get_combined_verdict(self, session_key: str) -> Optional[Dict[str, Any]]:
        """Return the original immutable close verdict for a session."""
        key = str(session_key or "").strip()
        if not key:
            return None
        conn = self._connect(busy_timeout_ms=500)
        try:
            row = conn.execute(
                "SELECT * FROM evidence_combined_session_verdicts WHERE decision_lane=? AND session_key=?",
                (self.decision_lane, key),
            ).fetchone()
            return None if row is None else self._decode_combined_payload(row)
        finally:
            conn.close()

    def final_seals_for_session(self, session_key: str) -> List[Dict[str, Any]]:
        """Return every immutable final epoch for this lane/session, including recovery."""
        key = str(session_key or "").strip()
        if not key:
            return []
        conn = self._connect(busy_timeout_ms=500)
        try:
            rows = conn.execute(
                """
                SELECT s.evidence_scope,s.epoch_token
                FROM evidence_seals s
                JOIN evidence_epochs e
                  ON e.evidence_scope=s.evidence_scope AND e.epoch_token=s.epoch_token
                WHERE e.decision_lane=? AND s.session_id=?
                  AND s.status IN ('SEALED_VALID','SEALED_INVALID')
                ORDER BY s.cutoff_sequence,s.evidence_scope,s.epoch_token
                """,
                (self.decision_lane, key),
            ).fetchall()
        finally:
            conn.close()
        return [self.verify_epoch(str(row["evidence_scope"]), str(row["epoch_token"])) for row in rows]

    def _validate_combined_referents(
        self, *, session_key: str, market_key: str, market_session_date: str,
        epoch_ids: Sequence[str], epoch_hashes: Sequence[str],
    ) -> List[Dict[str, Any]]:
        if not epoch_ids:
            raise HistoricalSealError("COMBINED_VERDICT_HISTORICAL_REFERENCE_REQUIRED")
        finals = self.final_seals_for_session(session_key)
        by_epoch = {str(item.get("epoch_token") or ""): item for item in finals}
        if len(by_epoch) != len(finals):
            raise HistoricalSealError("COMBINED_VERDICT_DUPLICATE_EPOCH_REFERENCE")
        validated: List[Dict[str, Any]] = []
        for epoch_id, epoch_hash in zip(epoch_ids, epoch_hashes):
            eid = str(epoch_id or "").strip(); digest = str(epoch_hash or "").strip()
            if not eid or not digest:
                raise HistoricalSealError("COMBINED_VERDICT_EMPTY_HISTORICAL_REFERENCE")
            item = by_epoch.get(eid)
            if item is None:
                raise HistoricalSealError(f"COMBINED_VERDICT_EPOCH_NOT_FOUND:{eid}")
            if not bool(item.get("passed")):
                raise HistoricalSealError(f"COMBINED_VERDICT_EPOCH_VERIFY_FAILED:{eid}")
            if str(item.get("seal_bundle_hash") or "") != digest:
                raise HistoricalSealError(f"COMBINED_VERDICT_EPOCH_HASH_MISMATCH:{eid}")
            if str(item.get("session_id") or "") != str(session_key):
                raise HistoricalSealError(f"COMBINED_VERDICT_EPOCH_SESSION_MISMATCH:{eid}")
            if str(item.get("market_session_date") or "") != str(market_session_date):
                raise HistoricalSealError(f"COMBINED_VERDICT_EPOCH_DATE_MISMATCH:{eid}")
            scope_market = normalize_market_key(str(item.get("evidence_scope") or "").partition("::")[2])
            if scope_market.casefold() != normalize_market_key(market_key).casefold():
                raise HistoricalSealError(f"COMBINED_VERDICT_EPOCH_MARKET_MISMATCH:{eid}")
            validated.append(dict(item))
        if set(by_epoch) != {str(value) for value in epoch_ids}:
            raise HistoricalSealError("COMBINED_VERDICT_OMITS_FINAL_SESSION_EPOCH")
        return validated

    def finalize_combined_verdict(
        self, *, session_key: str, market_key: str, market_session_date: str,
        evidence_session_id: str, shadow_seal_hash: str,
        historical_epoch_ids: Sequence[str], historical_epoch_seal_hashes: Sequence[str],
        combined_payload: Mapping[str, Any], producer_generation: Optional[int] = None,
        producer_instance_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Persist one immutable lane-scoped verdict only from real final seals."""
        key = str(session_key or "").strip()
        market = str(market_key or "").strip().upper()
        date = str(market_session_date or "").strip()
        evidence_id = str(evidence_session_id or key).strip()
        if not key or not market or not date or not evidence_id:
            raise HistoricalSealError("COMBINED_VERDICT_IDENTITY_REQUIRED")
        if not key.endswith(date):
            raise HistoricalSealError("COMBINED_VERDICT_SESSION_DATE_MISMATCH")
        shadow_hash = str(shadow_seal_hash or "").strip()
        if not shadow_hash:
            raise HistoricalSealError("COMBINED_VERDICT_SHADOW_HASH_REQUIRED")
        epoch_ids = [str(value) for value in historical_epoch_ids]
        epoch_hashes = [str(value) for value in historical_epoch_seal_hashes]
        if len(epoch_ids) != len(epoch_hashes):
            raise HistoricalSealError("COMBINED_VERDICT_HISTORICAL_LENGTH_MISMATCH")
        payload = dict(combined_payload or {})
        shadow_result = payload.get("shadow_result")
        if not isinstance(shadow_result, Mapping):
            raise HistoricalSealError("COMBINED_VERDICT_SHADOW_RESULT_REQUIRED")
        if _sha256_bytes(canonical_json_bytes(dict(shadow_result))) != shadow_hash:
            raise HistoricalSealError("COMBINED_VERDICT_SHADOW_HASH_MISMATCH")
        referents = self._validate_combined_referents(
            session_key=key, market_key=market, market_session_date=date,
            epoch_ids=epoch_ids, epoch_hashes=epoch_hashes,
        )
        if bool(payload.get("sealed_valid")) and (
            not bool(shadow_result.get("sealed_valid"))
            or not all(bool(item.get("sealed_valid")) for item in referents)
        ):
            raise HistoricalSealError("COMBINED_VERDICT_VALIDITY_CONTRADICTS_REFERENTS")
        payload["session_key"] = key
        payload["decision_lane"] = self.decision_lane
        payload["live_cutover_allowed"] = False
        payload["phase3_allowed"] = False
        blob = canonical_json_bytes(payload); digest = _sha256_bytes(blob)
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            generation, instance = self._assert_producer(conn, producer_generation, producer_instance_id)
            existing = conn.execute(
                "SELECT * FROM evidence_combined_session_verdicts WHERE decision_lane=? AND session_key=?",
                (self.decision_lane, key),
            ).fetchone()
            if existing is not None:
                conn.execute("COMMIT")
                return self._decode_combined_payload(existing)
            for eid, expected_hash in zip(epoch_ids, epoch_hashes):
                rows = conn.execute(
                    """SELECT s.status,s.seal_bundle_hash,e.status AS epoch_status,e.decision_lane
                       FROM evidence_seals s JOIN evidence_epochs e
                       ON e.evidence_scope=s.evidence_scope AND e.epoch_token=s.epoch_token
                       WHERE s.epoch_token=? AND s.session_id=? AND e.decision_lane=?""",
                    (eid, key, self.decision_lane),
                ).fetchall()
                if len(rows) != 1:
                    raise HistoricalSealError(f"COMBINED_VERDICT_REFERENT_CARDINALITY:{eid}")
                row0 = rows[0]
                if str(row0["status"] or "") not in FINAL_SEAL_STATUSES:
                    raise HistoricalSealError(f"COMBINED_VERDICT_REFERENT_NOT_FINAL:{eid}")
                if str(row0["status"] or "") != str(row0["epoch_status"] or ""):
                    raise HistoricalSealError(f"COMBINED_VERDICT_REFERENT_STATUS_SPLIT:{eid}")
                if str(row0["seal_bundle_hash"] or "") != str(expected_hash):
                    raise HistoricalSealError(f"COMBINED_VERDICT_REFERENT_HASH_CHANGED:{eid}")
            now = utc_now_text()
            conn.execute(
                """
                INSERT INTO evidence_combined_session_verdicts(
                    session_key,decision_lane,market_key,market_session_date,evidence_session_id,
                    producer_generation,producer_instance_id,shadow_seal_hash,
                    historical_epoch_ids_blob,historical_epoch_seal_hashes_blob,
                    combined_valid,combined_reason,combined_payload_blob,combined_payload_hash,finalized_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (key,self.decision_lane,market,date,evidence_id,generation,instance,shadow_hash,
                 canonical_json_bytes(epoch_ids),canonical_json_bytes(epoch_hashes),
                 1 if bool(payload.get("sealed_valid")) else 0,
                 str(payload.get("reason") or "") or None,blob,digest,now),
            )
            row = conn.execute(
                "SELECT * FROM evidence_combined_session_verdicts WHERE decision_lane=? AND session_key=?",
                (self.decision_lane,key),
            ).fetchone()
            conn.execute("COMMIT")
            if row is None:
                raise HistoricalSealError("COMBINED_VERDICT_INSERT_FAILED")
            return self._decode_combined_payload(row)
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _assert_producer(
        self,
        conn: sqlite3.Connection,
        generation: Optional[int],
        instance_id: Optional[str],
    ) -> Tuple[int, str]:
        supplied_generation = int(
            self.identity.generation if generation is None else generation
        )
        supplied_instance = str(
            self.identity.instance_id if instance_id is None else instance_id
        )
        row = conn.execute(
            "SELECT generation,instance_id FROM evidence_producers WHERE decision_lane=?",
            (self.decision_lane,),
        ).fetchone()
        if row is None:
            raise ProducerFenceError("PRODUCER_IDENTITY_MISSING")
        if int(row["generation"]) != supplied_generation:
            raise ProducerFenceError("STALE_PRODUCER_GENERATION")
        if str(row["instance_id"]) != supplied_instance:
            raise ProducerFenceError("STALE_PRODUCER_INSTANCE")
        return supplied_generation, supplied_instance

    @staticmethod
    def _new_epoch_token(scope: str) -> str:
        return "A112EPOCH-" + _sha256_text(
            f"{scope}|{utc_now_text()}|{time.time_ns()}|{uuid.uuid4().hex}"
        )

    def _ensure_open_epoch(
        self,
        conn: sqlite3.Connection,
        *,
        scope: str,
        generation: int,
        instance_id: str,
        session_identity: Optional[Mapping[str, str]] = None,
    ) -> str:
        identity = dict(session_identity or {})
        current = conn.execute(
            "SELECT epoch_token FROM evidence_current_epochs WHERE evidence_scope=?",
            (scope,),
        ).fetchone()
        if current is not None:
            token = str(current["epoch_token"])
            epoch = conn.execute(
                "SELECT * FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?",
                (scope, token),
            ).fetchone()
            if epoch is not None and str(epoch["status"]) == OPEN:
                existing_key = str(epoch["session_scope_key"] or "")
                requested_key = str(identity.get("session_scope_key") or "")
                if existing_key and requested_key and existing_key != requested_key:
                    raise HistoricalSealError(
                        f"CROSS_SESSION_INGRESS_REQUIRES_CLOSE:{existing_key}:{requested_key}"
                    )
                if not existing_key and requested_key:
                    count = int(conn.execute(
                        "SELECT COUNT(*) FROM evidence_ingress WHERE evidence_scope=? AND epoch_token=?",
                        (scope, token),
                    ).fetchone()[0])
                    if count:
                        raise HistoricalSealError("UNBOUND_EPOCH_WITH_EXISTING_INGRESS")
                    conn.execute(
                        """
                        UPDATE evidence_epochs SET market_key=?,market_session_date=?,
                            session_scope_key=?,evidence_session_id=?,session_id=?
                        WHERE evidence_scope=? AND epoch_token=? AND status='OPEN'
                        """,
                        (identity.get("market_key"), identity.get("market_session_date"),
                         requested_key, identity.get("evidence_session_id"),
                         identity.get("market_session_id"), scope, token),
                    )
                return token
        token = self._new_epoch_token(scope)
        now = utc_now_text()
        conn.execute(
            """
            INSERT INTO evidence_epochs(
                evidence_scope,epoch_token,decision_lane,status,opened_at,
                producer_generation,producer_instance_id,market_key,market_session_date,
                session_scope_key,evidence_session_id,session_id
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (scope, token, self.decision_lane, OPEN, now, generation, instance_id,
             identity.get("market_key"), identity.get("market_session_date"),
             identity.get("session_scope_key"), identity.get("evidence_session_id"),
             identity.get("market_session_id")),
        )
        conn.execute(
            """
            INSERT INTO evidence_current_epochs(evidence_scope,decision_lane,epoch_token,updated_at)
            VALUES(?,?,?,?)
            ON CONFLICT(evidence_scope) DO UPDATE SET
                decision_lane=excluded.decision_lane,
                epoch_token=excluded.epoch_token,
                updated_at=excluded.updated_at
            """,
            (scope, self.decision_lane, token, now),
        )
        return token

    def accept(
        self,
        kind: str,
        payload: Mapping[str, Any],
        *,
        operation_id: Optional[str] = None,
        producer_generation: Optional[int] = None,
        producer_instance_id: Optional[str] = None,
        lock_timeout_ms: Optional[int] = None,
    ) -> IngressReceipt:
        kind_text = str(kind or "").strip().lower()
        if kind_text not in KINDS:
            raise HistoricalSealError(f"INVALID_EVIDENCE_KIND:{kind_text}")
        if not isinstance(payload, Mapping):
            raise HistoricalSealError("PAYLOAD_MUST_BE_MAPPING")
        raw_payload = dict(payload or {})
        signal_id = str(raw_payload.get("id") or raw_payload.get("signal_id") or "").strip()
        if not signal_id:
            raise HistoricalSealError("SIGNAL_ID_REQUIRED")
        identity = session_identity_for_payload(raw_payload)
        scope = evidence_scope_for(self.decision_lane, raw_payload)
        if str(scope).partition("::")[2] != str(identity["market_key"]):
            raise HistoricalSealError("SESSION_MARKET_SCOPE_MISMATCH")
        op_id = str(operation_id or operation_id_for(
            kind_text, raw_payload, decision_lane=self.decision_lane
        )).strip()
        if not op_id:
            raise HistoricalSealError("OPERATION_ID_REQUIRED")
        envelope = {
            "kind": kind_text,
            "signal_id": signal_id,
            "decision_lane": self.decision_lane,
            "evidence_scope": scope,
            "session_identity": identity,
            "payload": raw_payload,
        }
        blob = canonical_json_bytes(envelope)
        payload_hash = _sha256_bytes(blob)
        accepted_at = utc_now_text()
        conn = self._connect(busy_timeout_ms=lock_timeout_ms)
        try:
            conn.execute("BEGIN IMMEDIATE")
            generation, instance_id = self._assert_producer(
                conn, producer_generation, producer_instance_id
            )
            existing = conn.execute(
                "SELECT * FROM evidence_ingress WHERE operation_id=?", (op_id,)
            ).fetchone()
            if existing is not None:
                if (
                    str(existing["canonical_payload_hash"]) != payload_hash
                    or str(existing["kind"]) != kind_text
                    or str(existing["evidence_scope"]) != scope
                    or str(existing["session_scope_key"] or "") != identity["session_scope_key"]
                ):
                    raise OperationPayloadMismatch("OPERATION_ID_PAYLOAD_MISMATCH")
                conn.execute("COMMIT")
                return IngressReceipt(
                    accept_sequence=int(existing["accept_sequence"]),
                    evidence_scope=str(existing["evidence_scope"]),
                    epoch_token=str(existing["epoch_token"]),
                    operation_id=op_id,
                    canonical_payload_hash=payload_hash,
                    accepted_at=str(existing["accepted_at"]),
                    producer_generation=int(existing["producer_generation"]),
                    producer_instance_id=str(existing["producer_instance_id"]),
                    idempotent_replay=True,
                )
            epoch_token = self._ensure_open_epoch(
                conn, scope=scope, generation=generation, instance_id=instance_id,
                session_identity=identity,
            )
            cursor = conn.execute(
                """
                INSERT INTO evidence_ingress(
                    evidence_scope,epoch_token,operation_id,canonical_payload_blob,
                    canonical_payload_hash,kind,signal_id,accepted_at,
                    producer_generation,producer_instance_id,market_key,
                    market_session_date,session_scope_key,evidence_session_id
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    scope, epoch_token, op_id, blob, payload_hash, kind_text,
                    signal_id, accepted_at, generation, instance_id,
                    identity["market_key"], identity["market_session_date"],
                    identity["session_scope_key"], identity["evidence_session_id"],
                ),
            )
            sequence = int(cursor.lastrowid)
            conn.execute("COMMIT")
            return IngressReceipt(
                accept_sequence=sequence,
                evidence_scope=scope,
                epoch_token=epoch_token,
                operation_id=op_id,
                canonical_payload_hash=payload_hash,
                accepted_at=accepted_at,
                producer_generation=generation,
                producer_instance_id=instance_id,
                idempotent_replay=False,
            )
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def ingress_receipt(self, accept_sequence: int) -> Optional[Dict[str, Any]]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM evidence_ingress WHERE accept_sequence=?",
                (int(accept_sequence),),
            ).fetchone()
            return dict(row) if row is not None else None
        finally:
            conn.close()

    def record_attempt(
        self,
        accept_sequence: int,
        *,
        side: str,
        result: str,
        result_payload: Optional[Mapping[str, Any]] = None,
        error: Optional[Any] = None,
        terminal: bool = False,
        terminal_status: Optional[str] = None,
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
        lock_timeout_ms: Optional[int] = None,
    ) -> Dict[str, Any]:
        side_text = str(side or "").strip().upper()
        if side_text not in SIDES:
            raise HistoricalSealError("INVALID_WRITER_SIDE")
        result_text = str(result or "").strip().upper()
        if not result_text:
            raise HistoricalSealError("ATTEMPT_RESULT_REQUIRED")
        resolved_terminal_status = PENDING
        known_terminal_status: Optional[str] = None
        if result_text in _TERMINAL_SUCCESS_RESULTS:
            known_terminal_status = TERMINAL_SUCCESS
        elif result_text in _TERMINAL_FAILURE_RESULTS:
            known_terminal_status = TERMINAL_FAILURE
        if terminal:
            supplied = str(terminal_status or "").strip().upper()
            if supplied and supplied not in TERMINAL_STATUSES:
                raise HistoricalSealError("INVALID_TERMINAL_STATUS")
            if supplied and known_terminal_status and supplied != known_terminal_status:
                raise HistoricalSealError(
                    f"TERMINAL_STATUS_RESULT_MISMATCH:{result_text}:{supplied}:{known_terminal_status}"
                )
            if supplied:
                resolved_terminal_status = supplied
            elif known_terminal_status:
                resolved_terminal_status = known_terminal_status
            else:
                raise HistoricalSealError(f"EXPLICIT_TERMINAL_STATUS_REQUIRED:{result_text}")
        elif terminal_status not in (None, "", PENDING):
            raise HistoricalSealError("NONTERMINAL_ATTEMPT_CANNOT_HAVE_TERMINAL_STATUS")
        started = str(started_at or utc_now_text())
        finished = str(finished_at or utc_now_text())
        result_blob = canonical_json_bytes(dict(result_payload or {})) if result_payload is not None else None
        result_hash = _sha256_bytes(result_blob) if result_blob is not None else None
        error_hash = _sha256_text(f"{type(error).__name__}:{error}") if error is not None else None
        conn = self._connect(busy_timeout_ms=lock_timeout_ms)
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._assert_producer(conn, None, None)
            ingress = conn.execute(
                "SELECT * FROM evidence_ingress WHERE accept_sequence=?", (int(accept_sequence),)
            ).fetchone()
            if ingress is None:
                raise HistoricalSealError("INGRESS_SEQUENCE_NOT_FOUND")
            epoch_row = conn.execute(
                "SELECT status FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?",
                (str(ingress["evidence_scope"]), str(ingress["epoch_token"])),
            ).fetchone()
            if epoch_row is None or str(epoch_row["status"]) in FINAL_SEAL_STATUSES:
                raise HistoricalSealError("FINAL_EPOCH_RECEIPT_IMMUTABLE")
            status_field = "live_status" if side_text == "LIVE" else "shadow_status"
            result_blob_field = "live_result_blob" if side_text == "LIVE" else "shadow_result_blob"
            result_hash_field = "live_result_hash" if side_text == "LIVE" else "shadow_result_hash"
            if str(ingress[status_field]) in TERMINAL_STATUSES:
                raise HistoricalSealError("TERMINAL_RECEIPT_ALREADY_RECORDED")
            row = conn.execute(
                "SELECT COALESCE(MAX(attempt_number),0) AS n FROM evidence_execution_attempts "
                "WHERE accept_sequence=? AND side=?", (int(accept_sequence), side_text),
            ).fetchone()
            attempt_number = int(row["n"] or 0) + 1
            conn.execute(
                """
                INSERT INTO evidence_execution_attempts(
                    accept_sequence,side,attempt_number,result,terminal,terminal_status,
                    started_at,finished_at,result_blob,result_hash,error_hash
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (int(accept_sequence), side_text, attempt_number, result_text,
                 1 if terminal else 0, resolved_terminal_status, started, finished,
                 result_blob, result_hash, error_hash),
            )
            conn.execute(
                f"UPDATE evidence_ingress SET {status_field}=?, {result_blob_field}=?, {result_hash_field}=? "
                "WHERE accept_sequence=?",
                (resolved_terminal_status, result_blob, result_hash, int(accept_sequence)),
            )
            conn.execute("COMMIT")
            return {
                "accept_sequence": int(accept_sequence), "side": side_text,
                "attempt_number": attempt_number, "result": result_text,
                "terminal": bool(terminal), "terminal_status": resolved_terminal_status,
                "result_hash": result_hash, "error_hash": error_hash,
            }
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _barrier_row(
        self, conn: sqlite3.Connection, *, scope: str, side: str
    ) -> Optional[sqlite3.Row]:
        checkpoint_field = "live_checkpoint_hash" if side == "LIVE" else "shadow_checkpoint_hash"
        return conn.execute(
            f"""
            SELECT e.evidence_scope,e.epoch_token,e.cutoff_sequence,e.status
            FROM evidence_epochs e
            JOIN evidence_seals s
              ON s.evidence_scope=e.evidence_scope AND s.epoch_token=e.epoch_token
            WHERE e.evidence_scope=?
              AND e.status IN ('DRAINING','BUNDLE_PREPARED','SIDECAR_COMMITTED')
              AND s.{checkpoint_field} IS NULL
            ORDER BY e.closed_at,e.epoch_token
            LIMIT 1
            """,
            (scope,),
        ).fetchone()

    def can_apply(self, accept_sequence: int, side: str) -> bool:
        side_text = str(side or "").strip().upper()
        if side_text not in SIDES:
            raise HistoricalSealError("INVALID_WRITER_SIDE")
        conn = self._connect()
        try:
            ingress = conn.execute(
                "SELECT evidence_scope,epoch_token FROM evidence_ingress WHERE accept_sequence=?",
                (int(accept_sequence),),
            ).fetchone()
            if ingress is None:
                raise HistoricalSealError("INGRESS_SEQUENCE_NOT_FOUND")
            scope = str(ingress["evidence_scope"])
            epoch = str(ingress["epoch_token"])
            epoch_row = conn.execute(
                "SELECT status FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?",
                (scope, epoch),
            ).fetchone()
            if epoch_row is None:
                return False
            epoch_status = str(epoch_row["status"] or "")
            seal_row = conn.execute(
                "SELECT status,live_checkpoint_hash,shadow_checkpoint_hash FROM evidence_seals "
                "WHERE evidence_scope=? AND epoch_token=?", (scope, epoch),
            ).fetchone()
            if epoch_status == OPEN:
                if seal_row is not None:
                    return False
                barrier = self._barrier_row(conn, scope=scope, side=side_text)
                return barrier is None
            if seal_row is None or str(seal_row["status"] or "") != epoch_status:
                return False
            if epoch_status in FINAL_SEAL_STATUSES:
                return False
            checkpoint_field = "live_checkpoint_hash" if side_text == "LIVE" else "shadow_checkpoint_hash"
            if epoch_status in {DRAINING, BUNDLE_PREPARED, SIDECAR_COMMITTED}:
                return seal_row[checkpoint_field] is None
            return False
        finally:
            conn.close()

    def assert_can_apply(self, accept_sequence: int, side: str) -> None:
        if not self.can_apply(accept_sequence, side):
            raise HistoricalBarrierBlocked(
                f"{str(side).upper()}_BARRIER_BLOCKED:{int(accept_sequence)}"
            )

    def open_scopes(self) -> List[str]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT evidence_scope FROM evidence_current_epochs
                WHERE decision_lane=? ORDER BY evidence_scope
                """,
                (self.decision_lane,),
            ).fetchall()
            return [str(row["evidence_scope"]) for row in rows]
        finally:
            conn.close()

    @staticmethod
    def _extract_session_date(value: Any) -> str:
        match = re.search(r"(20\d{2}-\d{2}-\d{2})", str(value or ""))
        return str(match.group(1)) if match else ""

    def _normalized_session_identity(
        self, conn: sqlite3.Connection, *, scope: str, epoch: str,
        details: Optional[Mapping[str, Any]],
    ) -> Tuple[Dict[str, Any], bytes, str, str, str, str]:
        supplied = dict(details or {})
        epoch_row = conn.execute(
            "SELECT * FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?",
            (scope, epoch),
        ).fetchone()
        if epoch_row is None:
            raise HistoricalSealError("EPOCH_NOT_FOUND")
        market = str(epoch_row["market_key"] or str(scope).partition("::")[2] or "UNKNOWN_MARKET")
        session_date = str(epoch_row["market_session_date"] or "")
        session_id = str(epoch_row["session_id"] or "")
        session_scope_key = str(epoch_row["session_scope_key"] or "")
        evidence_session_id = str(epoch_row["evidence_session_id"] or "")
        supplied_market = str(supplied.get("market_key") or supplied.get("market") or "").strip()
        if supplied_market and supplied_market != market:
            raise HistoricalSealError("SESSION_MARKET_SCOPE_MISMATCH")
        supplied_date = self._extract_session_date(
            supplied.get("market_session_date") or supplied.get("session_date") or supplied.get("date")
        )
        supplied_id = str(supplied.get("session_id") or supplied.get("market_session_id") or "").strip()
        if supplied_id:
            id_date = self._extract_session_date(supplied_id)
            if supplied_date and id_date and supplied_date != id_date:
                raise HistoricalSealError("SESSION_ID_DATE_MISMATCH")
        if not session_date:
            session_date = supplied_date
        if not session_date:
            raise HistoricalSealError("MARKET_SESSION_DATE_REQUIRED")
        if supplied_date and supplied_date != session_date:
            raise HistoricalSealError("SESSION_DATE_EPOCH_MISMATCH")
        if not session_id:
            session_id = supplied_id or f"{market}:{session_date}"
        if supplied_id and supplied_id != session_id:
            raise HistoricalSealError("SESSION_ID_EPOCH_MISMATCH")
        if self._extract_session_date(session_id) and self._extract_session_date(session_id) != session_date:
            raise HistoricalSealError("SESSION_ID_DATE_MISMATCH")
        expected_scope_key = f"{market}:{session_date}"
        if session_scope_key and session_scope_key != expected_scope_key:
            raise HistoricalSealError("SESSION_SCOPE_KEY_MISMATCH")
        session_scope_key = expected_scope_key
        evidence_session_id = evidence_session_id or str(supplied.get("evidence_session_id") or session_id)
        # Every ingress row must agree with the epoch's immutable identity.
        mismatched = conn.execute(
            """
            SELECT accept_sequence FROM evidence_ingress
            WHERE evidence_scope=? AND epoch_token=? AND (
                COALESCE(market_key,'')<>? OR COALESCE(market_session_date,'')<>?
                OR COALESCE(session_scope_key,'')<>? OR COALESCE(evidence_session_id,'')<>?
            ) LIMIT 1
            """,
            (scope, epoch, market, session_date, session_scope_key, evidence_session_id),
        ).fetchone()
        if mismatched is not None:
            raise HistoricalSealError(f"CROSS_SESSION_INGRESS:{int(mismatched['accept_sequence'])}")
        identity = {
            "session_id": session_id,
            "market_session_date": session_date,
            "market_key": market,
            "decision_lane": self.decision_lane,
            "evidence_scope": scope,
            "session_scope_key": session_scope_key,
            "evidence_session_id": evidence_session_id,
        }
        identity_hash = _sha256_bytes(canonical_json_bytes(identity))
        normalized = dict(supplied)
        normalized.update(identity)
        normalized["session_identity_hash"] = identity_hash
        details_blob = canonical_json_bytes(normalized)
        details_hash = _sha256_bytes(details_blob)
        return normalized, details_blob, details_hash, session_id, session_date, identity_hash

    def _rotate_scopes_atomic(
        self, scopes: Sequence[str], *, details: Optional[Mapping[str, Any]] = None
    ) -> List[RotationReceipt]:
        requested = [str(scope or "").strip() for scope in scopes]
        if not requested:
            return []
        if len(set(requested)) != len(requested):
            raise HistoricalSealError("DUPLICATE_ROTATION_SCOPE")
        now = utc_now_text()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            generation, instance_id = self._assert_producer(conn, None, None)
            plans: List[Dict[str, Any]] = []
            # Preflight every scope before mutating any row.
            for scope in requested:
                if not scope.startswith(f"{self.decision_lane}::"):
                    raise HistoricalSealError("EVIDENCE_SCOPE_LANE_MISMATCH")
                unfinished = conn.execute(
                    "SELECT epoch_token,status FROM evidence_epochs WHERE evidence_scope=? "
                    "AND status IN ('DRAINING','BUNDLE_PREPARED','SIDECAR_COMMITTED') "
                    "ORDER BY closed_at,epoch_token LIMIT 1", (scope,),
                ).fetchone()
                if unfinished is not None:
                    raise HistoricalBarrierBlocked(
                        f"OLDER_HISTORICAL_EPOCH_UNFINISHED:{unfinished['epoch_token']}:{unfinished['status']}"
                    )
                current = conn.execute(
                    "SELECT epoch_token FROM evidence_current_epochs WHERE evidence_scope=?",
                    (scope,),
                ).fetchone()
                if current is None:
                    old_epoch = self._ensure_open_epoch(
                        conn, scope=scope, generation=generation, instance_id=instance_id
                    )
                else:
                    old_epoch = str(current["epoch_token"])
                epoch_row = conn.execute(
                    "SELECT status FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?",
                    (scope, old_epoch),
                ).fetchone()
                if epoch_row is None or str(epoch_row["status"]) != OPEN:
                    raise HistoricalSealError("CURRENT_EPOCH_NOT_OPEN")
                row = conn.execute(
                    "SELECT COALESCE(MAX(accept_sequence),0) AS cutoff FROM evidence_ingress "
                    "WHERE evidence_scope=? AND epoch_token=?", (scope, old_epoch),
                ).fetchone()
                cutoff = int(row["cutoff"] or 0)
                normalized, details_blob, details_hash, session_id, session_date, identity_hash = (
                    self._normalized_session_identity(
                        conn, scope=scope, epoch=old_epoch, details=details
                    )
                )
                plans.append({
                    "scope": scope, "old_epoch": old_epoch, "new_epoch": self._new_epoch_token(scope),
                    "cutoff": cutoff, "details_blob": details_blob, "details_hash": details_hash,
                    "session_id": session_id, "session_date": session_date,
                    "identity_hash": identity_hash, "market_key": normalized["market_key"],
                    "session_scope_key": normalized["session_scope_key"],
                    "evidence_session_id": normalized["evidence_session_id"],
                })
            receipts: List[RotationReceipt] = []
            for plan in plans:
                cursor = conn.execute(
                    """
                    UPDATE evidence_epochs
                    SET status='DRAINING',closed_at=?,cutoff_sequence=?,session_details_blob=?,
                        session_details_hash=?,session_id=?,market_session_date=?,session_identity_hash=?,
                        market_key=?,session_scope_key=?,evidence_session_id=?
                    WHERE evidence_scope=? AND epoch_token=? AND status='OPEN'
                    """,
                    (now, plan["cutoff"], plan["details_blob"], plan["details_hash"],
                     plan["session_id"], plan["session_date"], plan["identity_hash"],
                     plan["market_key"], plan["session_scope_key"], plan["evidence_session_id"],
                     plan["scope"], plan["old_epoch"]),
                )
                if int(cursor.rowcount or 0) != 1:
                    raise HistoricalSealError("OPEN_EPOCH_ROTATION_FAILED")
                conn.execute(
                    """
                    INSERT INTO evidence_seals(
                        evidence_scope,epoch_token,cutoff_sequence,status,session_id,
                        market_session_date,session_identity_hash,session_details_hash
                    ) VALUES(?,?,?,'DRAINING',?,?,?,?)
                    """,
                    (plan["scope"], plan["old_epoch"], plan["cutoff"], plan["session_id"],
                     plan["session_date"], plan["identity_hash"], plan["details_hash"]),
                )
                conn.execute(
                    """
                    INSERT INTO evidence_epochs(
                        evidence_scope,epoch_token,decision_lane,status,opened_at,
                        producer_generation,producer_instance_id,market_key
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (plan["scope"], plan["new_epoch"], self.decision_lane, OPEN, now,
                     generation, instance_id, plan["market_key"]),
                )
                conn.execute(
                    "UPDATE evidence_current_epochs SET epoch_token=?,updated_at=? WHERE evidence_scope=?",
                    (plan["new_epoch"], now, plan["scope"]),
                )
                receipts.append(RotationReceipt(
                    evidence_scope=plan["scope"], closed_epoch_token=plan["old_epoch"],
                    new_epoch_token=plan["new_epoch"], cutoff_sequence=plan["cutoff"],
                    rotated_at=now, session_id=plan["session_id"],
                    market_session_date=plan["session_date"],
                    session_identity_hash=plan["identity_hash"],
                ))
            conn.execute("COMMIT")
            return receipts
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def rotate_scope(
        self,
        evidence_scope: str,
        *,
        details: Optional[Mapping[str, Any]] = None,
    ) -> RotationReceipt:
        results = self._rotate_scopes_atomic([str(evidence_scope)], details=details)
        if len(results) != 1:
            raise HistoricalSealError("ROTATION_RESULT_COUNT_INVALID")
        return results[0]

    def rotate_all_open_scopes(
        self, *, details: Optional[Mapping[str, Any]] = None
    ) -> List[RotationReceipt]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT c.evidence_scope,e.market_key
                FROM evidence_current_epochs c
                JOIN evidence_epochs e
                  ON e.evidence_scope=c.evidence_scope AND e.epoch_token=c.epoch_token
                WHERE c.decision_lane=? AND e.status='OPEN'
                ORDER BY c.evidence_scope
                """,
                (self.decision_lane,),
            ).fetchall()
        finally:
            conn.close()
        scopes = [str(row["evidence_scope"]) for row in rows]
        supplied_market = normalize_market_key(
            (details or {}).get("market_key") or (details or {}).get("market") or ""
        ) if ((details or {}).get("market_key") or (details or {}).get("market")) else ""
        if supplied_market and scopes:
            filtered = [scope for scope in scopes
                        if normalize_market_key(str(scope).partition("::")[2]) == supplied_market]
            if not filtered:
                raise HistoricalSealError("SESSION_MARKET_SCOPE_MISMATCH")
            scopes = filtered
        return self._rotate_scopes_atomic(scopes, details=details)

    def _terminal_rows(
        self, conn: sqlite3.Connection, *, scope: str, epoch: str, side: str
    ) -> List[Dict[str, Any]]:
        status_field = "live_status" if side == "LIVE" else "shadow_status"
        result_hash_field = "live_result_hash" if side == "LIVE" else "shadow_result_hash"
        rows = conn.execute(
            f"""
            SELECT accept_sequence,operation_id,kind,signal_id,{status_field} AS terminal_status,
                   {result_hash_field} AS result_hash,canonical_payload_hash
            FROM evidence_ingress
            WHERE evidence_scope=? AND epoch_token=?
            ORDER BY accept_sequence
            """,
            (scope, epoch),
        ).fetchall()
        return [dict(row) for row in rows]

    def checkpoint_side(
        self,
        evidence_scope: str,
        epoch_token: str,
        *,
        side: str,
        projection: Any,
    ) -> Dict[str, Any]:
        side_text = str(side or "").strip().upper()
        if side_text not in SIDES:
            raise HistoricalSealError("INVALID_WRITER_SIDE")
        scope = str(evidence_scope)
        epoch = str(epoch_token)
        if isinstance(projection, list):
            projection = {"ledger": list(projection), "events": []}
        if not isinstance(projection, Mapping):
            raise HistoricalSealError("CHECKPOINT_PROJECTION_MUST_BE_MAPPING")
        projection_value = dict(projection)
        if not isinstance(projection_value.get("ledger", []), list) or not isinstance(projection_value.get("events", []), list):
            raise HistoricalSealError("CHECKPOINT_PROJECTION_SCHEMA_INVALID")
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._assert_producer(conn, None, None)
            seal = conn.execute(
                "SELECT * FROM evidence_seals WHERE evidence_scope=? AND epoch_token=?", (scope, epoch),
            ).fetchone()
            epoch_row = conn.execute(
                "SELECT * FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?", (scope, epoch),
            ).fetchone()
            if seal is None or epoch_row is None:
                raise HistoricalSealError("SEAL_NOT_FOUND")
            if str(seal["status"] or "") != str(epoch_row["status"] or ""):
                raise HistoricalVerificationError("EPOCH_SEAL_STATUS_MISMATCH")
            if str(seal["status"] or "") in FINAL_SEAL_STATUSES:
                raise HistoricalSealError("FINAL_SEAL_IMMUTABLE")
            raw_mismatches = self._validate_raw_evidence(conn, scope=scope, epoch=epoch)
            if raw_mismatches:
                raise HistoricalVerificationError("RAW_EVIDENCE_INVALID:" + ",".join(raw_mismatches))
            status_field = "live_status" if side_text == "LIVE" else "shadow_status"
            pending = conn.execute(
                f"SELECT COUNT(*) AS n FROM evidence_ingress WHERE evidence_scope=? AND epoch_token=? "
                f"AND {status_field} NOT IN (?,?)",
                (scope, epoch, TERMINAL_SUCCESS, TERMINAL_FAILURE),
            ).fetchone()
            if int(pending["n"] or 0) != 0:
                raise CheckpointNotReady(f"{side_text}_TERMINAL_RECEIPTS_PENDING")
            receipts = self._terminal_rows(conn, scope=scope, epoch=epoch, side=side_text)
            material = {
                "seal_schema_version": SEAL_SCHEMA_VERSION,
                "canonicalization_version": CANONICALIZATION_VERSION,
                "evidence_scope": scope, "epoch_token": epoch,
                "cutoff_sequence": int(seal["cutoff_sequence"]),
                "session_id": str(seal["session_id"] or ""),
                "market_session_date": str(seal["market_session_date"] or ""),
                "session_identity_hash": str(seal["session_identity_hash"] or ""),
                "session_details_hash": str(seal["session_details_hash"] or ""),
                "side": side_text, "terminal_receipts": receipts,
                "projection": projection_value,
            }
            blob = canonical_json_bytes(material)
            digest = _sha256_bytes(blob)
            blob_field = "live_checkpoint_blob" if side_text == "LIVE" else "shadow_checkpoint_blob"
            hash_field = "live_checkpoint_hash" if side_text == "LIVE" else "shadow_checkpoint_hash"
            existing_hash = seal[hash_field]
            if existing_hash is not None:
                if str(existing_hash) != digest:
                    raise HistoricalVerificationError(f"{side_text}_CHECKPOINT_REWRITE_MISMATCH")
                conn.execute("COMMIT")
                return {"evidence_scope": scope, "epoch_token": epoch, "side": side_text,
                        "checkpoint_hash": digest, "checkpoint_bytes": len(blob),
                        "terminal_receipts": len(receipts), "barrier_released": True,
                        "idempotent_replay": True}
            conn.execute(
                f"UPDATE evidence_seals SET {blob_field}=?,{hash_field}=? "
                "WHERE evidence_scope=? AND epoch_token=?",
                (blob, digest, scope, epoch),
            )
            conn.execute("COMMIT")
            return {"evidence_scope": scope, "epoch_token": epoch, "side": side_text,
                    "checkpoint_hash": digest, "checkpoint_bytes": len(blob),
                    "terminal_receipts": len(receipts), "barrier_released": True,
                    "idempotent_replay": False}
        except Exception:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    @staticmethod
    def _hash_query_rows(rows: Iterable[sqlite3.Row], columns: Sequence[str]) -> str:
        material = [
            {column: row[column] for column in columns}
            for row in rows
        ]
        return _sha256_bytes(canonical_json_bytes(material))

    def _raw_evidence_material(
        self, conn: sqlite3.Connection, *, scope: str, epoch: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
        mismatches: List[str] = []
        ingress_material: List[Dict[str, Any]] = []
        rows = conn.execute(
            "SELECT * FROM evidence_ingress WHERE evidence_scope=? AND epoch_token=? ORDER BY accept_sequence",
            (scope, epoch),
        ).fetchall()
        for row in rows:
            seq = int(row["accept_sequence"])
            payload_blob = bytes(row["canonical_payload_blob"] or b"")
            payload_hash = _sha256_bytes(payload_blob) if payload_blob else ""
            if not payload_blob:
                mismatches.append(f"MISSING_CANONICAL_PAYLOAD_BLOB:{seq}")
            if payload_hash != str(row["canonical_payload_hash"] or ""):
                mismatches.append(f"CANONICAL_PAYLOAD_HASH_MISMATCH:{seq}")
            try:
                envelope = json.loads(payload_blob.decode("utf-8")) if payload_blob else {}
            except Exception:
                envelope = {}
                mismatches.append(f"CANONICAL_PAYLOAD_JSON_INVALID:{seq}")
            if envelope:
                if str(envelope.get("kind") or "") != str(row["kind"] or ""):
                    mismatches.append(f"PAYLOAD_KIND_MISMATCH:{seq}")
                if str(envelope.get("signal_id") or "") != str(row["signal_id"] or ""):
                    mismatches.append(f"PAYLOAD_SIGNAL_ID_MISMATCH:{seq}")
                if str(envelope.get("evidence_scope") or "") != str(row["evidence_scope"] or ""):
                    mismatches.append(f"PAYLOAD_SCOPE_MISMATCH:{seq}")
            item = {
                "accept_sequence": seq, "operation_id": str(row["operation_id"]),
                "canonical_payload_hash": payload_hash, "kind": str(row["kind"]),
                "signal_id": str(row["signal_id"]), "accepted_at": str(row["accepted_at"]),
                "producer_generation": int(row["producer_generation"]),
                "producer_instance_id": str(row["producer_instance_id"]),
                "live_status": str(row["live_status"]), "shadow_status": str(row["shadow_status"]),
            }
            for side in ("live", "shadow"):
                blob = bytes(row[f"{side}_result_blob"] or b"")
                stored = str(row[f"{side}_result_hash"] or "")
                actual = _sha256_bytes(blob) if blob else ""
                if bool(blob) != bool(stored) or (blob and actual != stored):
                    mismatches.append(f"{side.upper()}_RESULT_HASH_MISMATCH:{seq}")
                item[f"{side}_result_hash"] = actual
            ingress_material.append(item)
        attempt_material: List[Dict[str, Any]] = []
        attempts = conn.execute(
            """
            SELECT a.* FROM evidence_execution_attempts a
            JOIN evidence_ingress i ON i.accept_sequence=a.accept_sequence
            WHERE i.evidence_scope=? AND i.epoch_token=?
            ORDER BY a.accept_sequence,a.side,a.attempt_number
            """, (scope, epoch),
        ).fetchall()
        for row in attempts:
            seq = int(row["accept_sequence"]); side = str(row["side"]); num = int(row["attempt_number"])
            blob = bytes(row["result_blob"] or b"")
            stored = str(row["result_hash"] or "")
            actual = _sha256_bytes(blob) if blob else ""
            if bool(blob) != bool(stored) or (blob and actual != stored):
                mismatches.append(f"ATTEMPT_RESULT_HASH_MISMATCH:{seq}:{side}:{num}")
            terminal = bool(int(row["terminal"] or 0))
            terminal_status = str(row["terminal_status"] or PENDING)
            if terminal and terminal_status not in TERMINAL_STATUSES:
                mismatches.append(f"ATTEMPT_TERMINAL_STATUS_INVALID:{seq}:{side}:{num}")
            if not terminal and terminal_status != PENDING:
                mismatches.append(f"ATTEMPT_NONTERMINAL_STATUS_INVALID:{seq}:{side}:{num}")
            attempt_material.append({
                "accept_sequence": seq, "side": side, "attempt_number": num,
                "result": str(row["result"]), "terminal": int(row["terminal"] or 0),
                "terminal_status": terminal_status, "started_at": str(row["started_at"]),
                "finished_at": str(row["finished_at"]), "result_hash": actual,
                "error_hash": str(row["error_hash"] or ""),
            })
        return ingress_material, attempt_material, mismatches

    def _validate_raw_evidence(self, conn: sqlite3.Connection, *, scope: str, epoch: str) -> List[str]:
        return self._raw_evidence_material(conn, scope=scope, epoch=epoch)[2]

    def _historical_hashes(
        self, conn: sqlite3.Connection, *, scope: str, epoch: str
    ) -> Tuple[str, str]:
        ingress, attempts, _ = self._raw_evidence_material(conn, scope=scope, epoch=epoch)
        return (_sha256_bytes(canonical_json_bytes(ingress)),
                _sha256_bytes(canonical_json_bytes(attempts)))

    @staticmethod
    def _comparison_failure_count(value: Mapping[str, Any]) -> int:
        normalized = SessionHistoricalSeal._validate_comparison_schema(value)
        return sum(int(normalized[key]) for key in _COMPARISON_COUNT_KEYS)

    @staticmethod
    def _validate_comparison_schema(value: Mapping[str, Any]) -> Dict[str, Any]:
        if not isinstance(value, Mapping):
            raise HistoricalSealError("COMPARISON_MUST_BE_MAPPING")
        expected = set(_COMPARISON_COUNT_KEYS) | {"mismatch_details"}
        actual = {str(key) for key in value.keys()}
        if actual != expected:
            missing = sorted(expected - actual); extra = sorted(actual - expected)
            raise HistoricalSealError(f"COMPARISON_SCHEMA_INVALID:missing={missing}:extra={extra}")
        normalized: Dict[str, Any] = {}
        for key in _COMPARISON_COUNT_KEYS:
            item = value.get(key)
            if isinstance(item, bool) or not isinstance(item, int) or item < 0:
                raise HistoricalSealError(f"COMPARISON_COUNT_INVALID:{key}")
            normalized[key] = int(item)
        details = value.get("mismatch_details")
        if not isinstance(details, list):
            raise HistoricalSealError("COMPARISON_DETAILS_INVALID")
        normalized["mismatch_details"] = [dict(item) if isinstance(item, Mapping) else {"detail": str(item)} for item in details]
        return normalized

    @staticmethod
    def _decode_json_mapping(value: Any, *, field: str = "payload_json") -> Dict[str, Any]:
        if isinstance(value, Mapping):
            return dict(value)
        if value in (None, "", b""):
            return {}
        try:
            if isinstance(value, (bytes, bytearray, memoryview)):
                value = bytes(value).decode("utf-8")
            parsed = json.loads(str(value))
        except Exception as exc:
            raise HistoricalVerificationError(f"JSON_MAPPING_INVALID:{field}:{type(exc).__name__}:{exc}") from exc
        if not isinstance(parsed, Mapping):
            raise HistoricalVerificationError(f"JSON_MAPPING_REQUIRED:{field}")
        return dict(parsed)

    @classmethod
    def _signal_truth(cls, row: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
        raw = dict(row or {})
        payload = cls._decode_json_mapping(raw.get("payload_json"))
        merged = dict(payload); merged.update({k: v for k, v in raw.items() if k != "payload_json"})
        sid = str(merged.get("id") or merged.get("signal_id") or "").strip()
        truth = {field: _strict_value(merged.get(field), field=field) for field in _FINANCIAL_TRUTH_FIELDS}
        presence_raw = merged.get("__field_presence__")
        presence_map: Dict[str, str] = {}
        if isinstance(presence_raw, list):
            for item in presence_raw:
                if isinstance(item, Mapping):
                    presence_map[str(item.get("field_name") or "")] = str(item.get("presence_state") or "")
        elif isinstance(presence_raw, Mapping):
            presence_map = {str(k): str(v) for k, v in dict(presence_raw).items()}
        truth["__field_presence__"] = presence_map
        truth["state"] = str(merged.get("state") or "")
        terminal_raw = merged.get("terminal")
        truth["terminal"] = (
            bool(terminal_raw) if terminal_raw is not None
            else truth["state"].upper() in _TERMINAL_LIFECYCLE_STATES
        )
        deleted_raw = merged.get("deleted")
        truth["deleted"] = (
            bool(deleted_raw) if deleted_raw is not None
            else truth["state"].upper() == "DELETED"
        )
        return sid, truth

    @classmethod
    def _event_truth(cls, row: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
        raw = dict(row or {})
        payload = cls._decode_json_mapping(raw.get("payload_json"))
        merged = dict(payload); merged.update({k: v for k, v in raw.items() if k != "payload_json"})
        sid = str(merged.get("signal_id") or merged.get("id") or "").strip()
        aliases = {
            "operation_id": merged.get("operation_id") or merged.get("command_id") or merged.get("logical_event_id") or "",
            "logical_event_id": merged.get("logical_event_id") or merged.get("operation_id") or merged.get("command_id") or "",
            "causal_occurrence_id": merged.get("causal_occurrence_id") or merged.get("logical_event_id") or merged.get("operation_id") or merged.get("command_id") or "",
            "event_type": merged.get("event_type") or merged.get("last_event") or "",
            "state_before": merged.get("state_before") or merged.get("old_state") or "",
            "state_after": merged.get("state_after") or merged.get("new_state") or merged.get("state") or "",
            "price": merged.get("price"),
            "exit_price": merged.get("exit_price"),
            "pnl_pct": merged.get("pnl_pct"),
            "reason_code": merged.get("reason_code") or merged.get("reason") or "",
            "entry_status": merged.get("entry_status") or "",
            "episode_id": merged.get("episode_id") or "",
            "episode_key": merged.get("episode_key") or "",
            "symbol": merged.get("symbol") or "",
            "market_key": merged.get("market_key") or merged.get("market") or "",
            "timeframe": merged.get("timeframe") or "",
            "side": merged.get("side") or "",
        }
        value = {field: _strict_value(aliases.get(field), field=field) for field in _EVENT_TRUTH_FIELDS}
        value["event_type"] = str(value.get("event_type") or "").upper()
        return sid, value

    def _decode_comparison_checkpoints(
        self, *, scope: str, epoch: str, live_blob: bytes, shadow_blob: bytes,
        expected_session_identity_hash: str,
    ) -> Tuple[Mapping[str, Any], Mapping[str, Any], List[Dict[str, Any]], int]:
        try:
            live_cp = json.loads(live_blob.decode("utf-8"))
            shadow_cp = json.loads(shadow_blob.decode("utf-8"))
        except Exception as exc:
            raise HistoricalVerificationError(
                f"CHECKPOINT_JSON_INVALID:{type(exc).__name__}:{exc}"
            ) from exc
        if not isinstance(live_cp, Mapping) or not isinstance(shadow_cp, Mapping):
            raise HistoricalVerificationError("CHECKPOINT_MAPPING_REQUIRED")
        details: List[Dict[str, Any]] = []
        leakage = 0
        for side, checkpoint in (("LIVE", live_cp), ("SHADOW", shadow_cp)):
            if str(checkpoint.get("session_identity_hash") or "") != expected_session_identity_hash:
                leakage += 1; details.append({"reason": "SESSION_IDENTITY_MISMATCH", "side": side})
            if (str(checkpoint.get("evidence_scope") or "") != scope
                    or str(checkpoint.get("epoch_token") or "") != epoch):
                leakage += 1; details.append({"reason": "CHECKPOINT_SCOPE_EPOCH_MISMATCH", "side": side})
        live_projection = live_cp.get("projection") or {}
        shadow_projection = shadow_cp.get("projection") or {}
        if not isinstance(live_projection, Mapping) or not isinstance(shadow_projection, Mapping):
            raise HistoricalVerificationError("CHECKPOINT_PROJECTION_MAPPING_REQUIRED")
        for side, projection in (("LIVE", live_projection), ("SHADOW", shadow_projection)):
            if not isinstance(projection.get("ledger", []), list):
                raise HistoricalVerificationError(f"CHECKPOINT_LEDGER_LIST_REQUIRED:{side}")
            if not isinstance(projection.get("events", []), list):
                raise HistoricalVerificationError(f"CHECKPOINT_EVENTS_LIST_REQUIRED:{side}")
            if not isinstance(projection.get("runtime", {}), Mapping):
                raise HistoricalVerificationError(f"CHECKPOINT_RUNTIME_MAPPING_REQUIRED:{side}")
        return live_projection, shadow_projection, details, leakage

    def _ledger_truth_index(
        self, rows: Iterable[Mapping[str, Any]], *, side: str
    ) -> Dict[str, Dict[str, Any]]:
        target: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            signal_id, truth = self._signal_truth(row)
            if not signal_id:
                raise HistoricalVerificationError(f"LEDGER_SIGNAL_ID_REQUIRED:{side}")
            if signal_id in target:
                raise HistoricalVerificationError(f"LEDGER_DUPLICATE_SIGNAL_ID:{side}:{signal_id}")
            target[signal_id] = truth
        return target

    def _ingress_expectations(
        self, conn: sqlite3.Connection, *, scope: str, epoch: str
    ) -> Tuple[List[sqlite3.Row], Dict[str, Dict[str, Any]], set[str], Dict[str, Dict[str, Any]], List[str]]:
        rows = conn.execute(
            "SELECT accept_sequence,kind,signal_id,operation_id,canonical_payload_blob,live_status,shadow_status "
            "FROM evidence_ingress WHERE evidence_scope=? AND epoch_token=? ORDER BY accept_sequence",
            (scope, epoch),
        ).fetchall()
        upserts: Dict[str, Dict[str, Any]] = {}; deleted: set[str] = set()
        events: Dict[str, Dict[str, Any]] = {}; observations: List[str] = []
        for row in rows:
            try:
                envelope = json.loads(bytes(row["canonical_payload_blob"] or b"").decode("utf-8"))
            except Exception as exc:
                raise HistoricalVerificationError(
                    f"INGRESS_ENVELOPE_JSON_INVALID:{int(row['accept_sequence'])}:{type(exc).__name__}:{exc}"
                ) from exc
            if not isinstance(envelope, Mapping) or not isinstance(envelope.get("payload"), Mapping):
                raise HistoricalVerificationError(
                    f"INGRESS_ENVELOPE_MAPPING_REQUIRED:{int(row['accept_sequence'])}"
                )
            payload = dict(envelope["payload"]); kind = str(row["kind"] or "")
            signal_id = str(row["signal_id"] or "")
            both_success = (str(row["live_status"]) == TERMINAL_SUCCESS
                            and str(row["shadow_status"]) == TERMINAL_SUCCESS)
            if kind == "observation": observations.append(signal_id)
            if not both_success: continue
            if kind == "upsert": upserts[signal_id] = payload; deleted.discard(signal_id)
            elif kind == "transition":
                ledger = payload.get("ledger")
                event = payload.get("event")
                if not isinstance(ledger, Mapping) or not isinstance(event, Mapping):
                    raise HistoricalVerificationError(
                        f"INGRESS_TRANSITION_COMPONENTS_REQUIRED:{int(row['accept_sequence'])}"
                    )
                upserts[signal_id] = dict(ledger); deleted.discard(signal_id)
                operation_id = str(row["operation_id"] or "")
                events[operation_id] = {**dict(event), "signal_id": signal_id, "operation_id": operation_id}
            elif kind == "delete": upserts.pop(signal_id, None); deleted.add(signal_id)
            elif kind == "event":
                operation_id = str(row["operation_id"] or "")
                events[operation_id] = {**payload, "signal_id": signal_id, "operation_id": operation_id}
        return list(rows), upserts, deleted, events, observations

    def _compare_ledger_effects(
        self, *, live_index: Mapping[str, Mapping[str, Any]],
        shadow_index: Mapping[str, Mapping[str, Any]],
        expected_upserts: Mapping[str, Mapping[str, Any]], expected_deleted: set[str],
        details: List[Dict[str, Any]],
    ) -> int:
        mismatches = 0
        for signal_id in sorted(set(live_index) | set(shadow_index)):
            if signal_id not in live_index or signal_id not in shadow_index:
                mismatches += 1
                details.append({"signal_id": signal_id, "reason": "LEDGER_SIDE_MISSING",
                                "live_present": signal_id in live_index,
                                "shadow_present": signal_id in shadow_index})
                continue
            fields = [field for field in _FINANCIAL_TRUTH_FIELDS
                      if canonical_json_bytes(live_index[signal_id].get(field))
                      != canonical_json_bytes(shadow_index[signal_id].get(field))]
            if fields:
                mismatches += 1
                details.append({"signal_id": signal_id, "reason": "FINANCIAL_TRUTH_MISMATCH",
                                "fields": fields})
        for signal_id, payload in sorted(expected_upserts.items()):
            expected_fields = {
                field: _strict_value(payload.get(field), field=field)
                for field in _INGRESS_IMMUTABLE_TRUTH_FIELDS
                if field in payload and payload.get(field) is not None
            }
            for side, index in (("LIVE", live_index), ("SHADOW", shadow_index)):
                actual = index.get(signal_id)
                if actual is None:
                    mismatches += 1
                    details.append({"signal_id": signal_id, "reason": "EXPECTED_UPSERT_MISSING", "side": side})
                    continue
                presence = dict(actual.get("__field_presence__") or {})
                missing = [field for field in expected_fields
                           if presence.get(field) in {"MISSING", "PRESENT_NULL", "PRESENT_EMPTY"}]
                different = [field for field, value in expected_fields.items()
                             if field not in missing and
                             canonical_json_bytes(actual.get(field)) != canonical_json_bytes(value)]
                if missing or different:
                    mismatches += 1
                    details.append({"signal_id": signal_id,
                                    "reason": "INGRESS_CHECKPOINT_TRUTH_MISMATCH", "side": side,
                                    "missing_or_null_fields": missing, "different_fields": different})
        for signal_id in sorted(expected_deleted):
            live_present = signal_id in live_index and not bool(live_index[signal_id].get("deleted"))
            shadow_present = signal_id in shadow_index and not bool(shadow_index[signal_id].get("deleted"))
            if live_present or shadow_present:
                mismatches += 1
                details.append({"signal_id": signal_id, "reason": "EXPECTED_DELETE_NOT_REFLECTED",
                                "live_present": live_present, "shadow_present": shadow_present})
        return mismatches

    def _event_truth_index(
        self, rows: Iterable[Mapping[str, Any]], *, side: str
    ) -> Dict[str, List[Tuple[str, Dict[str, Any]]]]:
        target: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
        for row in rows:
            signal_id, value = self._event_truth(row)
            if not signal_id:
                raise HistoricalVerificationError(f"EVENT_SIGNAL_ID_REQUIRED:{side}")
            operation_id = str(value.get("operation_id") or "")
            if not operation_id:
                raise HistoricalVerificationError(f"EVENT_OPERATION_ID_REQUIRED:{side}:{signal_id}")
            target.setdefault(operation_id, []).append((signal_id, value))
        return target

    def _compare_event_occurrences(
        self, *, expected: Mapping[str, Mapping[str, Any]],
        live: Mapping[str, List[Tuple[str, Dict[str, Any]]]],
        shadow: Mapping[str, List[Tuple[str, Dict[str, Any]]]],
        details: List[Dict[str, Any]],
    ) -> int:
        mismatches = 0
        for operation_id in sorted(set(expected) | set(live) | set(shadow)):
            expected_event = dict(expected.get(operation_id) or {})
            left = list(live.get(operation_id, [])); right = list(shadow.get(operation_id, []))
            if len(left) != 1 or len(right) != 1:
                mismatches += 1
                details.append({"operation_id": operation_id,
                                "reason": "EVENT_OCCURRENCE_CARDINALITY_MISMATCH",
                                "expected": 1 if operation_id in expected else 0,
                                "live_count": len(left), "shadow_count": len(right)})
                continue
            live_id, live_truth = left[0]; shadow_id, shadow_truth = right[0]
            if live_id != shadow_id:
                mismatches += 1
                details.append({"operation_id": operation_id, "reason": "EVENT_SIGNAL_ID_MISMATCH",
                                "live_signal_id": live_id, "shadow_signal_id": shadow_id})
                continue
            expected_identity = {"operation_id": operation_id}
            for field in ("logical_event_id", "causal_occurrence_id"):
                value = str(expected_event.get(field) or "")
                if value: expected_identity[field] = value
            identity_diffs = [field for field, value in expected_identity.items()
                              if str(live_truth.get(field) or "") != value
                              or str(shadow_truth.get(field) or "") != value]
            if identity_diffs:
                mismatches += 1
                details.append({"operation_id": operation_id,
                                "reason": "EVENT_OCCURRENCE_IDENTITY_MISMATCH",
                                "fields": identity_diffs})
            elif canonical_json_bytes(live_truth) != canonical_json_bytes(shadow_truth):
                mismatches += 1
                details.append({"operation_id": operation_id,
                                "reason": "CRITICAL_EVENT_SEQUENCE_MISMATCH"})
        return mismatches

    def _internal_ledger_event_consistency(
        self, *, ledger_index: Mapping[str, Mapping[str, Any]],
        event_rows: Iterable[Mapping[str, Any]], side: str, details: List[Dict[str, Any]],
    ) -> int:
        state_events = {
            "ISSUED", "PENDING_NEXT_OPEN", "ENTRY_FILLED_NEXT_OPEN", "UPGRADED",
            "ACTIVATED", "NEAR_TARGET", "COOLING_EXTENDED", "TARGET_HIT",
            "STOP_HIT", "TIMEOUT", "ABORTED", "EOD_SQUARE_OFF", "DELETED",
            "STARTUP_SESSION_ROLLOVER", "SESSION_RESET",
        }
        latest: Dict[str, Dict[str, Any]] = {}
        for row in event_rows:
            signal_id, truth = self._event_truth(row)
            event_type = str(truth.get("event_type") or "").upper()
            if signal_id and event_type in state_events:
                latest[signal_id] = truth
        mismatches = 0
        for signal_id in sorted(set(ledger_index) | set(latest)):
            ledger = ledger_index.get(signal_id)
            event = latest.get(signal_id)
            if ledger is None or event is None:
                terminal = bool(ledger and str(ledger.get("state") or "").upper() in _TERMINAL_LIFECYCLE_STATES)
                terminal_event = bool(event and str(event.get("state_after") or "").upper() in _TERMINAL_LIFECYCLE_STATES)
                if terminal or terminal_event:
                    mismatches += 1
                    details.append({"side": side, "signal_id": signal_id,
                                    "reason": "LEDGER_EVENT_TERMINAL_PAIR_MISSING"})
                continue
            ledger_state = str(ledger.get("state") or "").upper()
            ledger_event = str(ledger.get("last_event") or "").upper()
            event_state = str(event.get("state_after") or "").upper()
            event_type = str(event.get("event_type") or "").upper()
            fields: List[str] = []
            if event_state and ledger_state != event_state:
                fields.append("state")
            if ledger_event and event_type and ledger_event != event_type:
                fields.append("last_event")
            if fields:
                mismatches += 1
                details.append({"side": side, "signal_id": signal_id,
                                "reason": "LEDGER_EVENT_STATE_CONTRADICTION",
                                "fields": fields, "ledger_state": ledger_state,
                                "event_state": event_state, "ledger_last_event": ledger_event,
                                "event_type": event_type})
        return mismatches

    def _runtime_comparison_counts(
        self, *, shadow_projection: Mapping[str, Any], ingress_rows: Iterable[sqlite3.Row],
        details: List[Dict[str, Any]],
    ) -> Tuple[int, int, int]:
        failures = sum(1 for row in ingress_rows
                       if str(row["live_status"]) == TERMINAL_FAILURE
                       or str(row["shadow_status"]) == TERMINAL_FAILURE)
        unfinished = sum(1 for row in ingress_rows
                         if str(row["live_status"]) not in TERMINAL_STATUSES
                         or str(row["shadow_status"]) not in TERMINAL_STATUSES)
        runtime = dict(shadow_projection.get("runtime") or {}); raw = runtime.get("mirror_drops", 0)
        if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
            failures += 1; mirror_drops = 0
            details.append({"reason": "INVALID_RUNTIME_COUNTER", "field": "mirror_drops",
                            "value": repr(raw)})
        else: mirror_drops = int(raw)
        process_invalid = runtime.get("process_invalid", False)
        if not isinstance(process_invalid, bool):
            failures += 1
            details.append({"reason": "INVALID_RUNTIME_BOOLEAN", "field": "process_invalid",
                            "value": repr(process_invalid)})
        elif process_invalid:
            failures += 1; details.append({"reason": "SHADOW_PROCESS_INVALID"})
        return failures, unfinished, mirror_drops

    def _compute_comparison(
        self, conn: sqlite3.Connection, *, scope: str, epoch: str,
        live_checkpoint_blob: bytes, shadow_checkpoint_blob: bytes,
        expected_session_identity_hash: str,
    ) -> Dict[str, Any]:
        live_projection, shadow_projection, details, leakage = self._decode_comparison_checkpoints(
            scope=scope, epoch=epoch, live_blob=live_checkpoint_blob,
            shadow_blob=shadow_checkpoint_blob,
            expected_session_identity_hash=expected_session_identity_hash,
        )
        live_index = self._ledger_truth_index(live_projection.get("ledger") or [], side="LIVE")
        shadow_index = self._ledger_truth_index(shadow_projection.get("ledger") or [], side="SHADOW")
        ingress, expected_upserts, expected_deleted, expected_events, observations = (
            self._ingress_expectations(conn, scope=scope, epoch=epoch)
        )
        financial = self._compare_ledger_effects(
            live_index=live_index, shadow_index=shadow_index,
            expected_upserts=expected_upserts, expected_deleted=expected_deleted,
            details=details,
        )
        live_events = self._event_truth_index(live_projection.get("events") or [], side="LIVE")
        shadow_events = self._event_truth_index(shadow_projection.get("events") or [], side="SHADOW")
        event_mismatches = self._compare_event_occurrences(
            expected=expected_events, live=live_events, shadow=shadow_events, details=details,
        )
        event_mismatches += self._internal_ledger_event_consistency(
            ledger_index=live_index, event_rows=live_projection.get("events") or [],
            side="LIVE", details=details,
        )
        event_mismatches += self._internal_ledger_event_consistency(
            ledger_index=shadow_index, event_rows=shadow_projection.get("events") or [],
            side="SHADOW", details=details,
        )
        known_ids = set(live_index) | set(shadow_index) | set(expected_upserts) | expected_deleted
        orphan = sum(1 for signal_id in observations if signal_id not in known_ids)
        failures, unfinished, mirror_drops = self._runtime_comparison_counts(
            shadow_projection=shadow_projection, ingress_rows=ingress, details=details,
        )
        return self._validate_comparison_schema({
            "financial_mismatches": int(financial), "event_mismatches": int(event_mismatches),
            "orphan_observations": int(orphan), "mirror_drops": int(mirror_drops),
            "processing_failures": int(failures), "unfinished_ingress": int(unfinished),
            "cross_session_leakage": int(leakage), "mismatch_details": details,
        })

    @staticmethod
    def _runtime_comparison_status(runtime_value: Any) -> Tuple[int, int, List[Dict[str, Any]]]:
        runtime = dict(runtime_value or {})
        failures = 0
        details: List[Dict[str, Any]] = []
        mirror_raw = runtime.get("mirror_drops", 0)
        if isinstance(mirror_raw, bool) or not isinstance(mirror_raw, int) or mirror_raw < 0:
            failures += 1
            mirror_drops = 0
            details.append({"reason": "INVALID_RUNTIME_COUNTER", "field": "mirror_drops",
                            "value": repr(mirror_raw)})
        else:
            mirror_drops = int(mirror_raw)
        process_invalid = runtime.get("process_invalid", False)
        if not isinstance(process_invalid, bool):
            failures += 1
            details.append({"reason": "INVALID_RUNTIME_BOOLEAN", "field": "process_invalid",
                            "value": repr(process_invalid)})
        elif process_invalid:
            failures += 1
            details.append({"reason": "SHADOW_PROCESS_INVALID"})
        return mirror_drops, failures, details

    def _sidecar_path(self, scope: str, epoch: str) -> Path:
        safe_scope = re.sub(r"[^A-Za-z0-9_.-]+", "_", scope)[:120]
        safe_epoch = re.sub(r"[^A-Za-z0-9_.-]+", "_", epoch)[:160]
        return self.sidecar_dir / f"{safe_scope}__{safe_epoch}.seal.json"

    @staticmethod
    def _atomic_write_verified(path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(path.name + f".{os.getpid()}.{time.time_ns()}.tmp")
        try:
            with open(temp, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
            # Best effort directory fsync where supported.
            try:
                fd = os.open(str(path.parent), os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            except Exception as exc:
                _report_suppressed_exception(
                    exc, module=__name__, file=__file__,
                    function='_atomic_write_verified', line=0,
                    stage='historical_sidecar_directory_fsync', critical=False,
                )
            reread = path.read_bytes()
            if reread != data:
                raise HistoricalVerificationError("SIDECAR_REREAD_MISMATCH")
        finally:
            try:
                if temp.exists():
                    temp.unlink()
            except Exception as exc:
                _report_suppressed_exception(
                    exc, module=__name__, file=__file__,
                    function='_atomic_write_verified', line=0,
                    stage='historical_sidecar_temp_cleanup', critical=False,
                )

    def finalize_seal(
        self,
        evidence_scope: str,
        epoch_token: str,
        *,
        comparison: Optional[Mapping[str, Any]] = None,
    ) -> Dict[str, Any]:
        scope = str(evidence_scope); epoch = str(epoch_token)
        conn = self._connect(); bundle_blob: Optional[bytes] = None; bundle_hash = ""
        sidecar_path = self._sidecar_path(scope, epoch)
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._assert_producer(conn, None, None)
            seal = conn.execute("SELECT * FROM evidence_seals WHERE evidence_scope=? AND epoch_token=?", (scope, epoch)).fetchone()
            epoch_row = conn.execute("SELECT * FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?", (scope, epoch)).fetchone()
            if seal is None or epoch_row is None:
                raise HistoricalSealError("SEAL_NOT_FOUND")
            if str(seal["status"] or "") != str(epoch_row["status"] or ""):
                raise HistoricalVerificationError("EPOCH_SEAL_STATUS_MISMATCH")
            status = str(seal["status"] or "")
            older = conn.execute(
                "SELECT epoch_token,status FROM evidence_epochs WHERE evidence_scope=? "
                "AND epoch_token<>? AND closed_at IS NOT NULL "
                "AND (closed_at < ? OR (closed_at = ? AND epoch_token < ?)) "
                "AND status NOT IN ('SEALED_VALID','SEALED_INVALID') "
                "ORDER BY closed_at,epoch_token LIMIT 1",
                (scope, epoch, str(epoch_row["closed_at"] or ""), str(epoch_row["closed_at"] or ""), epoch),
            ).fetchone()
            if older is not None:
                raise HistoricalBarrierBlocked(
                    f"OLDER_HISTORICAL_EPOCH_UNFINISHED:{older['epoch_token']}:{older['status']}"
                )
            if status in FINAL_SEAL_STATUSES:
                raise HistoricalSealError("SEAL_ALREADY_FINAL")
            if status != DRAINING:
                raise HistoricalSealError(f"SEAL_NOT_FINALIZABLE_FROM:{status}")
            if not seal["live_checkpoint_blob"] or not seal["shadow_checkpoint_blob"]:
                raise CheckpointNotReady("BOTH_CHECKPOINTS_REQUIRED")
            raw_mismatches = self._validate_raw_evidence(conn, scope=scope, epoch=epoch)
            if raw_mismatches:
                raise HistoricalVerificationError("RAW_EVIDENCE_INVALID:" + ",".join(raw_mismatches))
            details_blob = bytes(epoch_row["session_details_blob"] or b"")
            details_hash = _sha256_bytes(details_blob) if details_blob else ""
            if not details_blob or details_hash != str(epoch_row["session_details_hash"] or "") or details_hash != str(seal["session_details_hash"] or ""):
                raise HistoricalVerificationError("SESSION_DETAILS_HASH_MISMATCH")
            identity_hash = str(seal["session_identity_hash"] or "")
            if not identity_hash or identity_hash != str(epoch_row["session_identity_hash"] or ""):
                raise HistoricalVerificationError("SESSION_IDENTITY_HASH_MISMATCH")
            computed = self._compute_comparison(
                conn, scope=scope, epoch=epoch,
                live_checkpoint_blob=bytes(seal["live_checkpoint_blob"]),
                shadow_checkpoint_blob=bytes(seal["shadow_checkpoint_blob"]),
                expected_session_identity_hash=identity_hash,
            )
            if comparison is not None:
                supplied = self._validate_comparison_schema(comparison)
                if canonical_json_bytes(supplied) != canonical_json_bytes(computed):
                    raise HistoricalVerificationError("CALLER_COMPARISON_MISMATCH")
            comparison_blob = canonical_json_bytes(computed)
            comparison_hash = _sha256_bytes(comparison_blob)
            ingress_hash, attempts_hash = self._historical_hashes(conn, scope=scope, epoch=epoch)
            failure_count = int(computed["processing_failures"])
            pending_count = int(computed["unfinished_ingress"])
            comparison_failure_count = self._comparison_failure_count(computed)
            valid = comparison_failure_count == 0
            bundle = {
                "seal_schema_version": SEAL_SCHEMA_VERSION,
                "canonicalization_version": CANONICALIZATION_VERSION,
                "field_policy_version": FIELD_POLICY_VERSION,
                "comparison_contract_version": COMPARISON_CONTRACT_VERSION,
                "application_build_id": self.application_build_id,
                "evidence_scope": scope, "epoch_token": epoch,
                "cutoff_sequence": int(seal["cutoff_sequence"]),
                "session_id": str(seal["session_id"] or ""),
                "market_session_date": str(seal["market_session_date"] or ""),
                "session_identity_hash": identity_hash,
                "session_details_hash": details_hash,
                "live_checkpoint_hash": str(seal["live_checkpoint_hash"]),
                "shadow_checkpoint_hash": str(seal["shadow_checkpoint_hash"]),
                "comparison_hash": comparison_hash, "comparison": computed,
                "ingress_hash": ingress_hash, "attempts_hash": attempts_hash,
                "terminal_failure_count": failure_count, "pending_count": pending_count,
                "comparison_failure_count": comparison_failure_count,
                "sealed_valid": bool(valid),
            }
            bundle_blob = canonical_json_bytes(bundle); bundle_hash = _sha256_bytes(bundle_blob)
            prepared_at = utc_now_text()
            cursor = conn.execute(
                """
                UPDATE evidence_seals SET status=?,comparison_blob=?,comparison_hash=?,ingress_hash=?,
                    attempts_hash=?,seal_bundle_blob=?,seal_bundle_hash=?,sidecar_path=?,
                    invalid_reason=?,prepared_at=?
                WHERE evidence_scope=? AND epoch_token=? AND status='DRAINING'
                """,
                (BUNDLE_PREPARED, comparison_blob, comparison_hash, ingress_hash, attempts_hash,
                 bundle_blob, bundle_hash, str(sidecar_path), None if valid else "SEAL_CONTENT_INVALID",
                 prepared_at, scope, epoch),
            )
            if int(cursor.rowcount or 0) != 1:
                raise HistoricalSealError("SEAL_PREPARE_RACE")
            conn.execute("UPDATE evidence_epochs SET status=? WHERE evidence_scope=? AND epoch_token=? AND status='DRAINING'",
                         (BUNDLE_PREPARED, scope, epoch))
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction: conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        assert bundle_blob is not None
        prepared_mismatches = self._prepared_mismatches(scope, epoch)
        if prepared_mismatches:
            self.invalidate_epoch(scope, epoch, "PREPARED_MATERIAL_INVALID:" + ",".join(prepared_mismatches))
            raise HistoricalVerificationError("PREPARED_MATERIAL_INVALID:" + ",".join(prepared_mismatches))
        try:
            self._atomic_write_verified(sidecar_path, bundle_blob)
        except Exception as exc:
            self.invalidate_epoch(scope, epoch, f"SIDECAR_WRITE_FAILED:{type(exc).__name__}:{exc}")
            raise
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._assert_producer(conn, None, None)
            seal = conn.execute("SELECT * FROM evidence_seals WHERE evidence_scope=? AND epoch_token=?", (scope, epoch)).fetchone()
            if seal is None or str(seal["status"] or "") != BUNDLE_PREPARED:
                raise HistoricalVerificationError("BUNDLE_STATE_CHANGED_BEFORE_SIDECAR_COMMIT")
            if bytes(seal["seal_bundle_blob"] or b"") != bundle_blob or str(seal["seal_bundle_hash"] or "") != bundle_hash:
                raise HistoricalVerificationError("BUNDLE_CHANGED_BEFORE_SIDECAR_COMMIT")
            final_status = SEALED_INVALID if seal["invalid_reason"] else SEALED_VALID
            sealed_at = utc_now_text()
            conn.execute("UPDATE evidence_seals SET status=?,sealed_at=? WHERE evidence_scope=? AND epoch_token=? AND status='BUNDLE_PREPARED'",
                         (final_status, sealed_at, scope, epoch))
            conn.execute("UPDATE evidence_epochs SET status=? WHERE evidence_scope=? AND epoch_token=? AND status='BUNDLE_PREPARED'",
                         (final_status, scope, epoch))
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction: conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()
        verified = self.verify_epoch(scope, epoch)
        return {**verified, "sidecar_path": str(sidecar_path), "seal_bundle_hash": bundle_hash}

    def invalidate_epoch(self, evidence_scope: str, epoch_token: str, reason: str) -> None:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._assert_producer(conn, None, None)
            row = conn.execute("SELECT status FROM evidence_seals WHERE evidence_scope=? AND epoch_token=?",
                               (evidence_scope, epoch_token)).fetchone()
            if row is None:
                raise HistoricalSealError("SEAL_NOT_FOUND")
            if str(row["status"] or "") in FINAL_SEAL_STATUSES:
                raise HistoricalSealError("FINAL_SEAL_IMMUTABLE")
            conn.execute(
                "UPDATE evidence_seals SET status=?,invalid_reason=?,sealed_at=? WHERE evidence_scope=? AND epoch_token=?",
                (SEALED_INVALID, str(reason)[:2000], utc_now_text(), evidence_scope, epoch_token),
            )
            conn.execute("UPDATE evidence_epochs SET status=? WHERE evidence_scope=? AND epoch_token=?",
                         (SEALED_INVALID, evidence_scope, epoch_token))
            conn.execute("COMMIT")
        except Exception:
            if conn.in_transaction: conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _prepared_mismatches(self, scope: str, epoch: str) -> List[str]:
        conn = self._connect(); mismatches: List[str] = []
        try:
            seal = conn.execute("SELECT * FROM evidence_seals WHERE evidence_scope=? AND epoch_token=?", (scope, epoch)).fetchone()
            epoch_row = conn.execute("SELECT * FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?", (scope, epoch)).fetchone()
            if seal is None or epoch_row is None:
                return ["SEAL_NOT_FOUND"]
            if str(seal["status"] or "") != str(epoch_row["status"] or ""):
                mismatches.append("EPOCH_SEAL_STATUS_MISMATCH")
            for blob_field, hash_field in (("live_checkpoint_blob","live_checkpoint_hash"),
                                           ("shadow_checkpoint_blob","shadow_checkpoint_hash"),
                                           ("comparison_blob","comparison_hash"),
                                           ("seal_bundle_blob","seal_bundle_hash")):
                blob = bytes(seal[blob_field] or b""); digest = str(seal[hash_field] or "")
                if not blob: mismatches.append(f"MISSING:{blob_field}")
                elif _sha256_bytes(blob) != digest: mismatches.append(f"HASH_MISMATCH:{hash_field}")
            details_blob = bytes(epoch_row["session_details_blob"] or b"")
            details_hash = _sha256_bytes(details_blob) if details_blob else ""
            if not details_blob or details_hash != str(epoch_row["session_details_hash"] or "") or details_hash != str(seal["session_details_hash"] or ""):
                mismatches.append("SESSION_DETAILS_HASH_MISMATCH")
            if str(epoch_row["session_identity_hash"] or "") != str(seal["session_identity_hash"] or ""):
                mismatches.append("SESSION_IDENTITY_HASH_MISMATCH")
            mismatches.extend(self._validate_raw_evidence(conn, scope=scope, epoch=epoch))
            ingress_hash, attempts_hash = self._historical_hashes(conn, scope=scope, epoch=epoch)
            if ingress_hash != str(seal["ingress_hash"] or ""): mismatches.append("INGRESS_HASH_MISMATCH")
            if attempts_hash != str(seal["attempts_hash"] or ""): mismatches.append("ATTEMPTS_HASH_MISMATCH")
            bundle_blob = bytes(seal["seal_bundle_blob"] or b"")
            if bundle_blob:
                try: bundle = json.loads(bundle_blob.decode("utf-8"))
                except Exception: bundle = {}; mismatches.append("BUNDLE_JSON_INVALID")
                expected = {
                    "evidence_scope": scope, "epoch_token": epoch,
                    "cutoff_sequence": int(seal["cutoff_sequence"]),
                    "session_id": str(seal["session_id"] or ""),
                    "market_session_date": str(seal["market_session_date"] or ""),
                    "session_identity_hash": str(seal["session_identity_hash"] or ""),
                    "session_details_hash": str(seal["session_details_hash"] or ""),
                    "live_checkpoint_hash": str(seal["live_checkpoint_hash"] or ""),
                    "shadow_checkpoint_hash": str(seal["shadow_checkpoint_hash"] or ""),
                    "comparison_hash": str(seal["comparison_hash"] or ""),
                    "ingress_hash": ingress_hash, "attempts_hash": attempts_hash,
                    "seal_schema_version": SEAL_SCHEMA_VERSION,
                    "canonicalization_version": CANONICALIZATION_VERSION,
                    "field_policy_version": FIELD_POLICY_VERSION,
                    "comparison_contract_version": COMPARISON_CONTRACT_VERSION,
                    "application_build_id": self.application_build_id,
                }
                for key, value in expected.items():
                    if bundle.get(key) != value: mismatches.append(f"BUNDLE_FIELD_MISMATCH:{key}")
                try:
                    comparison = self._validate_comparison_schema(json.loads(bytes(seal["comparison_blob"]).decode("utf-8")))
                    if canonical_json_bytes(bundle.get("comparison")) != canonical_json_bytes(comparison):
                        mismatches.append("BUNDLE_COMPARISON_MISMATCH")
                    if int(bundle.get("comparison_failure_count", -1)) != self._comparison_failure_count(comparison):
                        mismatches.append("BUNDLE_COMPARISON_FAILURE_COUNT_MISMATCH")
                    if bool(bundle.get("sealed_valid")) != (self._comparison_failure_count(comparison) == 0):
                        mismatches.append("BUNDLE_VALIDITY_MISMATCH")
                except Exception as exc:
                    mismatches.append(f"COMPARISON_INVALID:{type(exc).__name__}:{exc}")
            return sorted(set(mismatches))
        finally:
            conn.close()

    def verify_epoch(self, evidence_scope: str, epoch_token: str) -> Dict[str, Any]:
        scope = str(evidence_scope); epoch = str(epoch_token)
        mismatches = self._prepared_mismatches(scope, epoch)
        conn = self._connect()
        try:
            seal = conn.execute("SELECT * FROM evidence_seals WHERE evidence_scope=? AND epoch_token=?", (scope, epoch)).fetchone()
            if seal is None:
                return {"passed": False, "sealed_valid": False, "mismatches": ["SEAL_NOT_FOUND"],
                        "evidence_scope": scope, "epoch_token": epoch}
            status = str(seal["status"] or "")
            epoch_row = conn.execute(
                "SELECT status FROM evidence_epochs WHERE evidence_scope=? AND epoch_token=?",
                (scope, epoch),
            ).fetchone()
            epoch_status = str(epoch_row["status"] or "") if epoch_row is not None else "MISSING"
            if epoch_status != status:
                mismatches.append("EPOCH_SEAL_STATUS_MISMATCH")
            bundle_blob = bytes(seal["seal_bundle_blob"] or b"")
            sidecar = Path(str(seal["sidecar_path"] or ""))
            if status in FINAL_SEAL_STATUSES:
                if not sidecar.is_file(): mismatches.append("SIDECAR_MISSING")
                elif sidecar.read_bytes() != bundle_blob: mismatches.append("SIDECAR_BYTES_MISMATCH")
            else:
                mismatches.append(f"SEAL_NOT_FINAL:{status}")
            bundle_valid = False
            if bundle_blob:
                try: bundle_valid = bool(json.loads(bundle_blob.decode("utf-8")).get("sealed_valid"))
                except Exception: bundle_valid = False
            mismatches = sorted(set(mismatches))
            passed = not mismatches and status in FINAL_SEAL_STATUSES
            sealed_valid = passed and status == SEALED_VALID and bundle_valid
            comparison = {}
            try:
                comparison = self._validate_comparison_schema(
                    json.loads(bytes(seal["comparison_blob"] or b"{}").decode("utf-8"))
                )
            except Exception:
                comparison = {}
            return {
                "passed": bool(passed), "sealed_valid": bool(sealed_valid), "status": status,
                "epoch_status": epoch_status,
                "mismatches": mismatches, "comparison": comparison,
                "evidence_scope": scope, "epoch_token": epoch,
                "cutoff_sequence": int(seal["cutoff_sequence"]),
                "session_id": str(seal["session_id"] or ""),
                "market_session_date": str(seal["market_session_date"] or ""),
                "session_identity_hash": str(seal["session_identity_hash"] or ""),
                "live_checkpoint_hash": str(seal["live_checkpoint_hash"] or ""),
                "shadow_checkpoint_hash": str(seal["shadow_checkpoint_hash"] or ""),
                "comparison_hash": str(seal["comparison_hash"] or ""),
                "ingress_hash": str(seal["ingress_hash"] or ""),
                "attempts_hash": str(seal["attempts_hash"] or ""),
                "seal_bundle_hash": str(seal["seal_bundle_hash"] or ""),
                "sidecar_path": str(sidecar), "live_cutover_allowed": False, "phase3_allowed": False,
            }
        finally:
            conn.close()

    def unfinished_epochs(self, evidence_scope: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            params: List[Any] = [f"{self.decision_lane}::%"]
            where = "e.evidence_scope LIKE ?"
            if evidence_scope is not None:
                where = "e.evidence_scope=?"
                params = [str(evidence_scope)]
            rows = conn.execute(
                f"""
                SELECT e.evidence_scope,e.epoch_token,e.status,e.closed_at,e.cutoff_sequence,
                       s.status AS seal_status,s.live_checkpoint_hash,s.shadow_checkpoint_hash
                FROM evidence_epochs e
                JOIN evidence_seals s ON s.evidence_scope=e.evidence_scope AND s.epoch_token=e.epoch_token
                WHERE {where} AND e.status IN ('DRAINING','BUNDLE_PREPARED','SIDECAR_COMMITTED')
                ORDER BY e.evidence_scope,e.closed_at,e.epoch_token
                """, tuple(params),
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            conn.close()

    def finalize_ready_epochs(self, evidence_scope: Optional[str] = None) -> Dict[str, Any]:
        completed: List[Dict[str, Any]] = []
        failures: List[Dict[str, Any]] = []
        for row in self.unfinished_epochs(evidence_scope):
            scope = str(row["evidence_scope"]); epoch = str(row["epoch_token"]); status = str(row["status"] or "")
            try:
                if status == DRAINING:
                    if not row.get("live_checkpoint_hash") or not row.get("shadow_checkpoint_hash"):
                        continue
                    completed.append(self.finalize_seal(scope, epoch))
                elif status in {BUNDLE_PREPARED, SIDECAR_COMMITTED}:
                    # The existing recovery path verifies prepared bytes before commit.
                    continue
            except CheckpointNotReady:
                continue
            except Exception as exc:
                failures.append({"evidence_scope": scope, "epoch_token": epoch,
                                 "error": f"{type(exc).__name__}:{exc}"})
        return {"completed": completed, "failures": failures,
                "remaining": self.unfinished_epochs(evidence_scope)}

    def recover_before_outbox_replay(self) -> Dict[str, Any]:
        conn = self._connect(); prepared: List[Tuple[str, str, bytes, str]] = []
        try:
            rows = conn.execute(
                "SELECT evidence_scope,epoch_token,status,seal_bundle_blob,sidecar_path FROM evidence_seals "
                "WHERE status IN ('DRAINING','BUNDLE_PREPARED','SIDECAR_COMMITTED') ORDER BY evidence_scope,epoch_token"
            ).fetchall()
            for row in rows:
                if str(row["status"]) == BUNDLE_PREPARED and row["seal_bundle_blob"]:
                    prepared.append((str(row["evidence_scope"]), str(row["epoch_token"]),
                                     bytes(row["seal_bundle_blob"]), str(row["sidecar_path"] or "")))
        finally:
            conn.close()
        completed: List[Dict[str, Any]] = []; failures: List[Dict[str, Any]] = []
        for scope, epoch, blob, path_text in prepared:
            mismatches = self._prepared_mismatches(scope, epoch)
            if mismatches:
                try:
                    self.invalidate_epoch(scope, epoch, "RECOVERY_PREPARED_INVALID:" + ",".join(mismatches))
                except Exception as exc:
                    _report_suppressed_exception(
                        exc, module=__name__, file=__file__,
                        function='recover_before_outbox_replay', line=0,
                        stage='historical_recovery_invalidation', critical=True,
                    )
                failures.append({"evidence_scope": scope, "epoch_token": epoch,
                                 "error": "RECOVERY_PREPARED_INVALID", "mismatches": mismatches})
                continue
            try:
                path = Path(path_text) if path_text else self._sidecar_path(scope, epoch)
                self._atomic_write_verified(path, blob)
                bundle = json.loads(blob.decode("utf-8"))
                status = SEALED_VALID if bool(bundle.get("sealed_valid")) else SEALED_INVALID
                conn = self._connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    self._assert_producer(conn, None, None)
                    cursor = conn.execute(
                        "UPDATE evidence_seals SET status=?,sidecar_path=?,sealed_at=? "
                        "WHERE evidence_scope=? AND epoch_token=? AND status='BUNDLE_PREPARED'",
                        (status, str(path), utc_now_text(), scope, epoch),
                    )
                    if int(cursor.rowcount or 0) != 1: raise HistoricalSealError("RECOVERY_STATE_RACE")
                    conn.execute("UPDATE evidence_epochs SET status=? WHERE evidence_scope=? AND epoch_token=? AND status='BUNDLE_PREPARED'",
                                 (status, scope, epoch))
                    conn.execute("COMMIT")
                except Exception:
                    if conn.in_transaction: conn.execute("ROLLBACK")
                    raise
                finally: conn.close()
                verified = self.verify_epoch(scope, epoch)
                if not verified.get("passed"):
                    raise HistoricalVerificationError("RECOVERED_SEAL_VERIFY_FAILED:" + ",".join(verified.get("mismatches") or []))
                completed.append({"evidence_scope": scope, "epoch_token": epoch, "status": status})
            except Exception as exc:
                failures.append({"evidence_scope": scope, "epoch_token": epoch,
                                 "error": f"{type(exc).__name__}:{exc}"})
        ready = self.finalize_ready_epochs()
        failures.extend(list(ready.get("failures") or []))
        return {"recovered_prepared": completed,
                "recovered_draining": list(ready.get("completed") or []),
                "recovery_failures": failures,
                "remaining_unfinished": list(ready.get("remaining") or []),
                "barriers": self.barrier_status(), "live_cutover_allowed": False, "phase3_allowed": False}

    def barrier_status(self) -> List[Dict[str, Any]]:
        conn = self._connect()
        try:
            rows = conn.execute(
                """
                SELECT e.evidence_scope,e.epoch_token,e.cutoff_sequence,e.status,
                       s.live_checkpoint_hash,s.shadow_checkpoint_hash
                FROM evidence_epochs e
                JOIN evidence_seals s
                  ON s.evidence_scope=e.evidence_scope AND s.epoch_token=e.epoch_token
                WHERE e.status IN ('DRAINING','BUNDLE_PREPARED','SIDECAR_COMMITTED')
                ORDER BY e.evidence_scope,e.closed_at
                """
            ).fetchall()
            return [{
                "evidence_scope": str(row["evidence_scope"]),
                "epoch_token": str(row["epoch_token"]),
                "cutoff_sequence": int(row["cutoff_sequence"] or 0),
                "status": str(row["status"]),
                "live_blocked": row["live_checkpoint_hash"] is None,
                "shadow_blocked": row["shadow_checkpoint_hash"] is None,
            } for row in rows]
        finally:
            conn.close()

    def signal_ids_for_epoch(self, evidence_scope: str, epoch_token: str) -> List[str]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT DISTINCT signal_id FROM evidence_ingress WHERE evidence_scope=? AND epoch_token=? ORDER BY signal_id",
                (str(evidence_scope), str(epoch_token)),
            ).fetchall()
            return [str(row[0]) for row in rows if str(row[0] or "")]
        finally:
            conn.close()

    def pending_envelopes(self, side: str) -> List[Dict[str, Any]]:
        side_text = str(side or "").strip().upper()
        if side_text not in SIDES: raise HistoricalSealError("INVALID_WRITER_SIDE")
        field = "live_status" if side_text == "LIVE" else "shadow_status"
        conn = self._connect()
        try:
            rows = conn.execute(
                f"SELECT * FROM evidence_ingress WHERE evidence_scope LIKE ? AND {field}='PENDING' ORDER BY accept_sequence",
                (f"{self.decision_lane}::%",),
            ).fetchall()
            result: List[Dict[str, Any]] = []
            for row in rows:
                blob = bytes(row["canonical_payload_blob"] or b"")
                if _sha256_bytes(blob) != str(row["canonical_payload_hash"] or ""):
                    raise HistoricalVerificationError(f"PENDING_PAYLOAD_HASH_MISMATCH:{int(row['accept_sequence'])}")
                envelope = json.loads(blob.decode("utf-8"))
                result.append({
                    "accept_sequence": int(row["accept_sequence"]),
                    "evidence_scope": str(row["evidence_scope"]),
                    "epoch_token": str(row["epoch_token"]),
                    "operation_id": str(row["operation_id"]),
                    "kind": str(row["kind"]),
                    "payload": dict(envelope.get("payload") or {}),
                    "producer_generation": int(row["producer_generation"]),
                    "producer_instance_id": str(row["producer_instance_id"]),
                })
            return result
        finally:
            conn.close()

    def pending_counts(self) -> Dict[str, int]:
        conn = self._connect()
        try:
            row = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN live_status='PENDING' THEN 1 ELSE 0 END) AS live_pending,
                    SUM(CASE WHEN shadow_status='PENDING' THEN 1 ELSE 0 END) AS shadow_pending,
                    SUM(CASE WHEN live_status='TERMINAL_FAILURE' THEN 1 ELSE 0 END) AS live_failures,
                    SUM(CASE WHEN shadow_status='TERMINAL_FAILURE' THEN 1 ELSE 0 END) AS shadow_failures
                FROM evidence_ingress
                WHERE evidence_scope LIKE ?
                """,
                (f"{self.decision_lane}::%",),
            ).fetchone()
            return {key: int(row[key] or 0) for key in row.keys()}
        finally:
            conn.close()

    def status(self) -> Dict[str, Any]:
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) AS n FROM evidence_ingress WHERE evidence_scope LIKE ?",
                (f"{self.decision_lane}::%",),
            ).fetchone()
            seals = conn.execute(
                """
                SELECT status,COUNT(*) AS n FROM evidence_seals
                WHERE evidence_scope LIKE ? GROUP BY status
                """,
                (f"{self.decision_lane}::%",),
            ).fetchall()
            return {
                "enabled": True,
                "decision_lane": self.decision_lane,
                "db_path": self.db_path,
                "producer": self.current_process_identity(),
                "ingress_count": int(row["n"] or 0),
                "pending": self.pending_counts(),
                "barriers": self.barrier_status(),
                "unfinished_epochs": self.unfinished_epochs(),
                "seal_counts": {str(item["status"]): int(item["n"]) for item in seals},
                "live_cutover_allowed": False,
                "phase3_allowed": False,
            }
        finally:
            conn.close()

    @staticmethod
    def snapshot_sqlite_rows(
        db_path: str,
        *,
        table: str,
        where_sql: str = "",
        parameters: Sequence[Any] = (),
        order_by: str = "",
    ) -> List[Dict[str, Any]]:
        table_name = str(table or "")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
            raise HistoricalSealError("INVALID_TABLE_NAME")
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        try:
            sql = f"SELECT * FROM {table_name}"
            if where_sql:
                sql += " WHERE " + str(where_sql)
            if order_by:
                sql += " ORDER BY " + str(order_by)
            return [dict(row) for row in conn.execute(sql, tuple(parameters)).fetchall()]
        finally:
            conn.close()


__all__ = [
    "APPLICATION_BUILD_ID", "BUNDLE_PREPARED", "CANONICALIZATION_VERSION",
    "CheckpointNotReady", "COMPARISON_CONTRACT_VERSION", "DRAINING",
    "env_historical_seal_enabled", "evidence_scope_for", "FIELD_POLICY_VERSION",
    "HistoricalBarrierBlocked", "historical_db_for_live_db", "HistoricalSealError",
    "HistoricalVerificationError", "IngressReceipt", "OPEN", "operation_id_for",
    "OperationPayloadMismatch", "ProducerFenceError", "ProducerIdentity",
    "RotationReceipt", "SEALED_INVALID", "SEALED_VALID", "SEAL_SCHEMA_VERSION",
    "SessionHistoricalSeal", "TERMINAL_FAILURE", "TERMINAL_SUCCESS", "FINAL_SEAL_STATUSES",
    "canonical_json_bytes", "canonical_json_text", "utc_now_text",
]
