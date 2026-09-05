# -*- coding: utf-8 -*-
"""H1R13 R4 F32 parent-bound probability resolution authority.

The isolated probability worker may validate and consume this authority, but it
must never synthesize it from its own model frame.  US promotion is fail-closed:
without a valid parent-bound session-pinned resolution authority, the result
remains PARTIAL_MARKET_PROVISIONAL even if the worker happens to see a 100%-full
subset of the market.
"""
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

import pandas as pd

from r159_pipeline_core import classify_probability_kind, official_coverage_decision

CONTRACT = "H1R13_R4_F32_PARENT_BOUND_RESOLUTION_AUTHORITY_V1"
US_RESOLUTION_CONTRACT = "US_TARGET_BAR_RESOLUTION_H1R13_R4_F32_V1"


def _is_us_market(market_key: Any) -> bool:
    text = str(market_key or "").strip().lower()
    return bool("الأمريك" in text or "american" in text or text in {"local_us", "us_local"})


def _bar_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        ts = pd.to_datetime(text, errors="coerce")
        if pd.isna(ts):
            return text[:16]
        if getattr(ts, "tzinfo", None) is not None:
            ts = ts.tz_localize(None)
        return pd.Timestamp(ts).floor("min").strftime("%Y-%m-%d %H:%M")
    except Exception:
        return text[:16]


def validate_parent_resolution_authority(
    authority: Optional[Mapping[str, Any]], *, market_key: str, bar_time: Any
) -> Dict[str, Any]:
    """Validate immutable parent evidence. Never infer missing fields in-worker."""
    auth = dict(authority or {})
    out: Dict[str, Any] = {
        "valid": False,
        "reason": "AUTHORITY_MISSING",
        "contract": str(auth.get("contract") or ""),
        "market_key": str(market_key or ""),
        "bar_key": _bar_key(bar_time),
    }
    if not auth:
        return out
    if str(auth.get("contract") or "") != CONTRACT:
        out["reason"] = "AUTHORITY_CONTRACT_MISMATCH"; return out
    if str(auth.get("built_by") or "").upper() != "PARENT":
        out["reason"] = "AUTHORITY_NOT_PARENT_BOUND"; return out
    if str(auth.get("market_key") or "") != str(market_key or ""):
        out["reason"] = "AUTHORITY_MARKET_MISMATCH"; return out
    if _bar_key(auth.get("bar_key")) != _bar_key(bar_time) or not _bar_key(bar_time):
        out["reason"] = "AUTHORITY_BAR_MISMATCH"; return out
    if not bool(auth.get("session_pinned")):
        out["reason"] = "AUTHORITY_SESSION_PIN_MISSING"; return out
    if not str(auth.get("session_key") or ""):
        out["reason"] = "AUTHORITY_SESSION_KEY_MISSING"; return out
    if not str(auth.get("universe_hash") or ""):
        out["reason"] = "AUTHORITY_UNIVERSE_HASH_MISSING"; return out
    try:
        total = int(auth.get("trusted_universe_count") or 0)
        exact = int(auth.get("exact_target_count") or 0)
        resolved = int(auth.get("resolved_count") or 0)
        no_new = int(auth.get("confirmed_no_new_bar_count") or 0)
        pending = int(auth.get("pending_count") or 0)
        scanned = int(auth.get("source_scan_scanned") or 0)
        scan_total = int(auth.get("source_scan_total") or 0)
    except Exception:
        out["reason"] = "AUTHORITY_COUNT_INVALID"; return out
    if total <= 0 or exact < 0 or exact > total:
        out["reason"] = "AUTHORITY_DENOMINATOR_INVALID"; return out
    if scan_total != total:
        out["reason"] = "AUTHORITY_SOURCE_SCAN_DENOMINATOR_MISMATCH"; return out
    if not bool(auth.get("source_scan_complete")):
        out["reason"] = "AUTHORITY_SOURCE_SCAN_INCOMPLETE"; return out
    if not bool(auth.get("resolution_complete")) or pending != 0:
        out["reason"] = "AUTHORITY_RESOLUTION_INCOMPLETE"; return out
    if resolved < total or scanned < total:
        out["reason"] = "AUTHORITY_RESOLVED_COUNT_INCOMPLETE"; return out
    if _is_us_market(market_key) and str(auth.get("resolution_contract") or "") != US_RESOLUTION_CONTRACT:
        out["reason"] = "AUTHORITY_US_RESOLUTION_CONTRACT_MISMATCH"; return out
    if exact + max(0, no_new) < total and resolved < total:
        out["reason"] = "AUTHORITY_ACCOUNTING_GAP"; return out
    ratio = float(exact) / float(total)
    if ratio + 1e-12 < 0.90:
        out["reason"] = "AUTHORITY_EXACT_COVERAGE_LT_90PCT"; return out
    out.update({
        "valid": True,
        "reason": "AUTHORITY_VALID",
        "trusted_universe_count": total,
        "exact_target_count": exact,
        "resolved_count": resolved,
        "confirmed_no_new_bar_count": max(0, no_new),
        "coverage_ratio": ratio,
        "coverage_pct": round(ratio * 100.0, 3),
        "resolution_contract": str(auth.get("resolution_contract") or ""),
        "session_key": str(auth.get("session_key") or ""),
        "universe_hash": str(auth.get("universe_hash") or ""),
    })
    return out


def classify_with_parent_resolution_authority(
    *, market_key: str, bar_time: Any, model_target_count: int, model_total_symbols: int,
    authority: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Return classification + coverage evidence without changing model rows."""
    validation = validate_parent_resolution_authority(authority, market_key=market_key, bar_time=bar_time)
    us_market = _is_us_market(market_key)
    if bool(validation.get("valid")):
        total = int(validation["trusted_universe_count"])
        exact = int(validation["exact_target_count"])
        decision = official_coverage_decision(
            exact, total,
            source_scan_complete=True,
            confirmed_no_new_bar=int(validation.get("confirmed_no_new_bar_count") or 0),
            resolved_count=int(validation.get("resolved_count") or 0),
            resolution_complete=True,
            resolved_exact_min_ratio=0.90,
        )
        kind = "MARKET_WIDE_CONFIRMED" if not bool(decision.get("coverage_hard_block")) else "PARTIAL_MARKET_PROVISIONAL"
        return {
            "probability_kind": kind,
            "coverage_target_count": exact,
            "coverage_total_symbols": total,
            "coverage_ratio": float(exact) / float(total),
            "authority_valid": True,
            "authority_reason": str(validation.get("reason") or ""),
            "authority_contract": CONTRACT,
            "authority_resolution_contract": str(validation.get("resolution_contract") or ""),
            "authority_session_key": str(validation.get("session_key") or ""),
            "authority_universe_hash": str(validation.get("universe_hash") or ""),
            "authority_resolved_count": int(validation.get("resolved_count") or 0),
            "authority_confirmed_no_new_bar_count": int(validation.get("confirmed_no_new_bar_count") or 0),
        }
    # F32: US worker-frame completeness is not market completeness. A missing or
    # malformed parent authority can never promote itself by recounting its subset.
    if us_market:
        kind = "PARTIAL_MARKET_PROVISIONAL"
    else:
        kind = classify_probability_kind(int(model_target_count or 0), int(model_total_symbols or 0))
    ratio = (float(model_target_count) / float(model_total_symbols)) if int(model_total_symbols or 0) > 0 else 0.0
    return {
        "probability_kind": kind,
        "coverage_target_count": int(model_target_count or 0),
        "coverage_total_symbols": int(model_total_symbols or 0),
        "coverage_ratio": ratio,
        "authority_valid": False,
        "authority_reason": str(validation.get("reason") or "AUTHORITY_MISSING"),
        "authority_contract": str((authority or {}).get("contract") or ""),
        "authority_resolution_contract": str((authority or {}).get("resolution_contract") or ""),
        "authority_session_key": str((authority or {}).get("session_key") or ""),
        "authority_universe_hash": str((authority or {}).get("universe_hash") or ""),
        "authority_resolved_count": int((authority or {}).get("resolved_count") or 0),
        "authority_confirmed_no_new_bar_count": int((authority or {}).get("confirmed_no_new_bar_count") or 0),
    }


__all__ = [
    "CONTRACT", "US_RESOLUTION_CONTRACT", "validate_parent_resolution_authority",
    "classify_with_parent_resolution_authority",
]
