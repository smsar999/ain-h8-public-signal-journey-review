# -*- coding: utf-8 -*-
"""Canonical content-generation identity for local MetaStock/TickerChart sources.

H12H14H5: mtime+size are discovery hints, not semantic truth. TickerChart can
replace/rewrite a DAT while preserving both values. The production source lane
therefore carries a content-generation token independently from the legacy
SourceSignature tuple so historical callers remain compatible.

This module also owns the canonical decoded-record dependency fingerprint used
by *both* PriceTape and SourcePriority identity paths. One physical generation
must have one identity regardless of which lane observes it first.
"""
from __future__ import annotations
import hashlib
import json
import math
from typing import Any, Iterable, Mapping
from live_sniper_source_signature import normalize_source_signature
from column_truth_contract import wall_time_key

VERSION = "A4_2_14_CORE_CAUSAL_TRUTH_HOTFIX12H14H5_V2"

def _finite_or_none(value: Any):
    try:
        out=float(value)
        return out if math.isfinite(out) else None
    except (TypeError, ValueError, OverflowError):
        return None

def records_dependency_sha256(records: Iterable[Mapping[str, Any]] | None) -> str:
    """Fingerprint the exact decoded technical-dependency rows.

    Canonicalization intentionally matches ``r1693_4_signal_close_authority``.
    The function lives here to keep PriceTape and SourcePriority from deriving
    different identities for the same completed DAT generation.
    """
    payload=[]
    for raw in (records or []):
        row=dict(raw or {})
        raw_time=(row.get("date") or row.get("datetime") or row.get("time")
                  or row.get("bar_datetime") or row.get("bar_time"))
        payload.append({
            "bar_datetime": wall_time_key(raw_time),
            "open": _finite_or_none(row.get("open")),
            "high": _finite_or_none(row.get("high")),
            "low": _finite_or_none(row.get("low")),
            "close": _finite_or_none(row.get("close")),
            "volume": _finite_or_none(row.get("volume")),
        })
    raw=json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(",",":"))
    return hashlib.sha256(raw.encode("utf-8","surrogatepass")).hexdigest()

def source_snapshot_identity(*, source_file: Any, source_mtime_ns: Any, source_size: Any, dependency_sha256: Any) -> str:
    raw="|".join([
        str(source_file or "").strip().replace("\\","/"),
        str(source_mtime_ns or "").strip(),
        str(source_size or "").strip(),
        str(dependency_sha256 or "").strip(),
    ])
    return hashlib.sha256(raw.encode("utf-8","surrogatepass")).hexdigest()

def generation_token(meta: Mapping[str, Any] | None) -> str:
    src = dict(meta or {})
    for key in (
        "source_generation_id", "source_snapshot_id", "source_dependency_sha256",
        "source_tail_sha256", "source_audit_fingerprint", "source_scan_fingerprint",
    ):
        value = str(src.get(key) or "").strip().lower()
        if value:
            return value
    return ""

def physical_generation_id(meta: Mapping[str, Any] | None, *, market: str = "", norm: str = "") -> str:
    src = dict(meta or {})
    sig = normalize_source_signature(src.get("signature") or src)
    token = generation_token(src)
    raw = "\x1f".join((str(market or ""), str(norm or ""), str(sig.mtime_ns), str(sig.size), token))
    return hashlib.sha256(raw.encode("utf-8", "surrogatepass")).hexdigest()

def same_physical_generation(left: Mapping[str, Any] | None, right: Mapping[str, Any] | None) -> bool:
    a = dict(left or {}); b = dict(right or {})
    sa = normalize_source_signature(a.get("signature") or a)
    sb = normalize_source_signature(b.get("signature") or b)
    if sa != sb:
        return False
    ta, tb = generation_token(a), generation_token(b)
    if ta or tb:
        return bool(ta and tb and ta == tb)
    return True

__all__ = [
    "VERSION", "generation_token", "physical_generation_id",
    "same_physical_generation", "records_dependency_sha256",
    "source_snapshot_identity",
]
