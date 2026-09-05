# -*- coding: utf-8 -*-
"""R1.2 SOURCE_PRIORITY lease accounting.

A selected physical source version is a lease until it reaches one explicit
terminal accounting fate.  H12H14H3 strengthens this registry for frozen-sample
collection: unknown outcomes, duplicate acquisition and completion without an
open lease are all visible and conservation is computed explicitly.

The registry remains diagnostic/runtime accounting; physical pending/source
files remain the recovery authority.
"""
from __future__ import annotations

import hashlib
import threading
import time
from source_generation_identity import generation_token
from typing import Any, Dict, Mapping

VERSION = "A4_2_14_CORE_CAUSAL_TRUTH_HOTFIX12H14H5_V1"


def _lease_id(market: str, norm: str, meta: Mapping[str, Any]) -> str:
    signature = tuple((meta or {}).get("signature") or (0, 0))
    token = generation_token(meta)
    identity = f"{market}\x1f{norm}\x1f{signature!r}"
    if token:
        identity += f"\x1f{token}"
    raw = identity.encode("utf-8", "replace")
    return hashlib.sha256(raw).hexdigest()


def _outcome_metric(outcome: str) -> str:
    """Map production outcome text to one exhaustive accounting class.

    Callers historically passed detailed reasons such as DEADLINE_DEFERRED or
    PROCESS_QUEUE_FULL_REQUEUED.  Treating every unknown string as ACKED hid
    source debt.  H3 classifies intent conservatively and leaves truly unknown
    values in an explicit error bucket.
    """
    value = str(outcome or "").strip().upper()
    if value == "ACKED" or value.endswith("_ACKED"):
        return "leases_acked"
    if "STALE_REJECT" in value:
        return "leases_stale_rejected"
    if "QUARANTIN" in value:
        return "leases_quarantined"
    if "RECOVER" in value:
        return "leases_recovered"
    if "SUPERSEDED" in value or "DOMINATED" in value:
        return "leases_superseded"
    if "REQUEUE" in value or "DEFER" in value or "RETRY" in value:
        return "leases_requeued"
    if "SHUTDOWN" in value or "CANCEL" in value or "STALE_SOURCE_PROCESSING_EPOCH" in value:
        return "leases_cancelled"
    return "lease_unknown_outcome"


_TERMINAL_METRICS = (
    "leases_acked",
    "leases_requeued",
    "leases_quarantined",
    "leases_stale_rejected",
    "leases_recovered",
    "leases_superseded",
    "leases_cancelled",
    "lease_unknown_outcome",
)


class SourceLeaseAccounting:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._open: Dict[str, Dict[str, Any]] = {}
        self._metrics: Dict[str, Dict[str, int]] = {}
        self._completion_without_open_samples: list[Dict[str, Any]] = []

    def _bucket(self, market: str) -> Dict[str, int]:
        return self._metrics.setdefault(str(market or ""), {
            "leases_acquired": 0,
            "leases_acked": 0,
            "leases_requeued": 0,
            "leases_quarantined": 0,
            "leases_stale_rejected": 0,
            "leases_recovered": 0,
            "leases_superseded": 0,
            "leases_cancelled": 0,
            "lease_duplicate_acquire": 0,
            "lease_completion_without_open": 0,
            "lease_open_duplicate_suppressed": 0,
            "lease_unknown_outcome": 0,
        })

    def acquire_many(self, market: str, leased: Mapping[str, Mapping[str, Any]]) -> None:
        key = str(market or "")
        with self._lock:
            bucket = self._bucket(key)
            now = time.monotonic()
            for norm, meta in dict(leased or {}).items():
                lid = _lease_id(key, str(norm), meta or {})
                if lid in self._open:
                    bucket["lease_duplicate_acquire"] += 1
                    continue
                self._open[lid] = {
                    "lease_id": lid,
                    "market": key,
                    "norm": str(norm),
                    "signature": tuple((meta or {}).get("signature") or (0, 0)),
                    "source_generation_id": generation_token(meta),
                    "acquired_monotonic": now,
                }
                bucket["leases_acquired"] += 1

    def claim_many(self, market: str, leased: Mapping[str, Mapping[str, Any]]) -> Dict[str, str]:
        """Idempotently claim source generations for the production lane."""
        key = str(market or "")
        result: Dict[str, str] = {}
        with self._lock:
            bucket = self._bucket(key)
            now = time.monotonic()
            for norm, meta in dict(leased or {}).items():
                lid = _lease_id(key, str(norm), meta or {})
                if lid in self._open:
                    bucket["lease_open_duplicate_suppressed"] += 1
                    result[str(norm)] = "ALREADY_OPEN"
                    continue
                self._open[lid] = {
                    "lease_id": lid,
                    "market": key,
                    "norm": str(norm),
                    "signature": tuple((meta or {}).get("signature") or (0, 0)),
                    "source_generation_id": generation_token(meta),
                    "acquired_monotonic": now,
                }
                bucket["leases_acquired"] += 1
                result[str(norm)] = "ACQUIRED"
        return result

    def complete(self, market: str, norm: str, meta: Mapping[str, Any], outcome: str) -> None:
        key = str(market or "")
        with self._lock:
            bucket = self._bucket(key)
            lid = _lease_id(key, str(norm or ""), meta or {})
            existed = lid in self._open
            if existed:
                self._open.pop(lid, None)
            else:
                bucket["lease_completion_without_open"] += 1
                self._completion_without_open_samples.append({
                    "lease_id": lid,
                    "market": key,
                    "norm": str(norm or ""),
                    "source_generation_id": generation_token(meta or {}),
                    "signature": tuple((meta or {}).get("signature") or (0, 0)),
                    "outcome": str(outcome or ""),
                    "thread": threading.current_thread().name,
                    "observed_monotonic": time.monotonic(),
                })
                if len(self._completion_without_open_samples) > 32:
                    del self._completion_without_open_samples[:-32]
                return
            metric = _outcome_metric(outcome)
            bucket[metric] += 1

    @staticmethod
    def _with_conservation(metrics: Dict[str, Any], open_count: int) -> Dict[str, Any]:
        terminal = sum(int(metrics.get(name, 0) or 0) for name in _TERMINAL_METRICS)
        acquired = int(metrics.get("leases_acquired", 0) or 0)
        delta = acquired - (terminal + int(open_count))
        metrics["lease_terminal_total"] = int(terminal)
        metrics["lease_conservation_delta"] = int(delta)
        metrics["lease_conservation_valid"] = bool(delta == 0)
        return metrics

    def snapshot(self, market: str = "") -> Dict[str, Any]:
        with self._lock:
            if market:
                key = str(market)
                metrics: Dict[str, Any] = dict(self._bucket(key))
                opened = [dict(item) for item in self._open.values() if item.get("market") == key]
            else:
                metrics = {}
                for bucket in self._metrics.values():
                    for name, value in bucket.items():
                        metrics[name] = int(metrics.get(name, 0)) + int(value)
                template = self._bucket("")
                for name in template:
                    metrics.setdefault(name, 0)
                opened = [dict(item) for item in self._open.values()]
            metrics["open_leases"] = len(opened)
            metrics["oldest_open_lease_age_sec"] = max(
                [max(0.0, time.monotonic() - float(item.get("acquired_monotonic") or time.monotonic())) for item in opened]
                or [0.0]
            )
            self._with_conservation(metrics, len(opened))
            samples = [dict(item) for item in self._completion_without_open_samples]
            if market:
                samples = [item for item in samples if str(item.get("market") or "") == str(market)]
            return {
                "version": VERSION, "metrics": metrics, "open": opened,
                "completion_without_open_samples": samples,
            }


def source_lease_accounting_for_owner(owner: Any) -> SourceLeaseAccounting:
    registry = getattr(owner, "_r12_source_lease_accounting", None)
    if registry is None:
        registry = SourceLeaseAccounting()
        setattr(owner, "_r12_source_lease_accounting", registry)
    return registry


__all__ = ["VERSION", "SourceLeaseAccounting", "source_lease_accounting_for_owner"]
