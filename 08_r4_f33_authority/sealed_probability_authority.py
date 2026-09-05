# -*- coding: utf-8 -*-
"""Exact authority for calling a probability *sealed*.

A radar/review row is evidence, not a seal. A score may be promoted to the
sealed lane only when an exact source-bar close and its lineage are present.
This module is pure and intentionally does not calculate any probability.
"""
from __future__ import annotations

import datetime as _dt
import math
from dataclasses import dataclass
from typing import Any, Mapping, Optional
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore
from market_datetime_normalizer import strict_market_timezone as _canonical_strict_market_timezone
from exception_observability import report_suppressed_exception as _report_suppressed_exception

VERSION = "A4_2_14_EXCEPTION_SEAL_OBSERVABILITY_HOTFIX12H8_SEALED_PROBABILITY_V5"
SEALED_SCOPE = "SEALED_CROSS_BAR"
RECONSTRUCTED_SCOPE = "RADAR_RECONSTRUCTED_UNSEALED"


def _text(value: Any) -> str:
    if value is None:
        return ""
    out = str(value).strip()
    return "" if out.lower() in {"nan", "none", "null", "nat"} else out


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        try:
            return bool(float(value)) and math.isfinite(float(value))
        except Exception:
            return False
    return _text(value).lower() in {"1", "true", "yes", "y", "on", "نعم", "صح"}


def _finite(value: Any) -> Optional[float]:
    try:
        out = float(value)
        return out if math.isfinite(out) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _dt_value(value: Any) -> Optional[_dt.datetime]:
    text = _text(value).replace("Z", "+00:00")
    if not text:
        return None
    try:
        return _dt.datetime.fromisoformat(text.replace("T", " "))
    except Exception:
        return None


def _market_timezone(row: Mapping[str, Any]):
    source = row or {}
    name = _text(source.get("market_timezone") or source.get("timezone"))
    market = _text(source.get("market_key") or source.get("decision_market_key"))
    if not name:
        return _canonical_strict_market_timezone(market)
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception as exc:
            _ = exc
    if name == "Asia/Riyadh":
        return _dt.timezone(_dt.timedelta(hours=3), name="Asia/Riyadh")
    raise ValueError(f"SEALED_MARKET_TIMEZONE_UNAVAILABLE:{name}")


def _utc_comparable(value: _dt.datetime, *, row: Mapping[str, Any]) -> _dt.datetime:
    if value.tzinfo is None:
        zone = _market_timezone(row)
        if hasattr(zone, "localize"):
            value = zone.localize(value, is_dst=None)  # type: ignore[attr-defined]
        else:
            value = value.replace(tzinfo=zone)
    return value.astimezone(_dt.timezone.utc)


def sealed_close_value(row: Mapping[str, Any]) -> Optional[float]:
    source = row or {}
    for key in (
        "sealed_signal_bar_close", "signal_bar_close", "signal_close",
        "sealed_close", "final_signal_bar_close",
    ):
        value = _finite(source.get(key))
        if value is not None:
            return value
    return None


def _lineage_present(row: Mapping[str, Any]) -> bool:
    source = row or {}
    source_id = _text(
        source.get("sealed_close_source_observation_id")
        or source.get("final_source_observation_id")
    )
    explicit_exact = bool(
        _truthy(source.get("sealed_close_verified"))
        or _truthy(source.get("exact_source_bar_materialized"))
        or _text(source.get("live_sniper_source_lane")).upper() == "EXACT_SEALED_BAR_RECONCILIATION"
        or _text(source.get("sealed_close_source")).upper() in {
            "EXACT_SOURCE_BAR", "EXACT_SEALED_BAR_RECONCILIATION",
            "SEALED_CROSS_BAR", "EXACT_SIGNAL_BAR_MATERIALIZATION",
        }
    )
    return bool(explicit_exact and source_id)


def _scope_claims_sealed(row: Mapping[str, Any]) -> bool:
    source = row or {}
    scope = _text(
        source.get("probability_scope_sealed")
        or source.get("probability_scope")
        or source.get("gann20_probability_scope")
    ).upper()
    state = _text(
        source.get("live_pulse_seal_state")
        or source.get("gann20_episode_state")
        or source.get("state")
    ).upper()
    return bool(
        scope == SEALED_SCOPE
        or state.startswith("SEALED_")
        or _truthy(source.get("signal_bar_is_sealed"))
        or _truthy(source.get("signal_bar_sealed"))
    )


def _seal_not_before_window_end(row: Mapping[str, Any]) -> Optional[bool]:
    source = row or {}
    window_end = _dt_value(source.get("forming_window_end"))
    if window_end is None:
        return True
    sealed_at = None
    for key in (
        "signal_bar_sealed_at", "sealed_at", "sealed_model_anchor_at",
        "probability_finished_at", "last_update", "signal_published_at",
    ):
        sealed_at = _dt_value(source.get(key))
        if sealed_at is not None:
            break
    if sealed_at is None:
        return True
    try:
        return _utc_comparable(sealed_at, row=source) >= _utc_comparable(window_end, row=source)
    except Exception as exc:
        _report_suppressed_exception(
            exc, module=__name__, file=__file__, function="_seal_not_before_window_end",
            line=0, stage="probability_attach", critical=True,
            operation="sealed_probability_time_authority",
            reason_code="SEALED_TIME_AUTHORITY_UNAVAILABLE",
        )
        return None


def resolve_confirmed_bar_key(
    *, explicit_confirmed_bar_key: Any, latest_bar_key: Any, previous_bar_key: Any,
    historical_or_stale: bool,
) -> str:
    explicit = _text(explicit_confirmed_bar_key)
    if explicit:
        return explicit
    latest = _text(latest_bar_key)
    if historical_or_stale:
        return latest
    return _text(previous_bar_key)


@dataclass(frozen=True)
class SealedProbabilityAuthority:
    allowed: bool
    reason_code: str
    close: Optional[float]


def sealed_probability_authority(row: Mapping[str, Any]) -> SealedProbabilityAuthority:
    close = sealed_close_value(row)
    if close is None:
        return SealedProbabilityAuthority(False, "SEALED_CLOSE_VALUE_MISSING", None)
    if not _lineage_present(row):
        return SealedProbabilityAuthority(False, "SEALED_CLOSE_LINEAGE_MISSING", close)
    if not _scope_claims_sealed(row):
        return SealedProbabilityAuthority(False, "SEALED_SCOPE_NOT_PROVEN", close)
    seal_time_ok = _seal_not_before_window_end(row)
    if seal_time_ok is None:
        return SealedProbabilityAuthority(False, "SEALED_TIME_AUTHORITY_UNAVAILABLE", close)
    if not seal_time_ok:
        return SealedProbabilityAuthority(False, "SEALED_BEFORE_FORMING_WINDOW_END", close)
    return SealedProbabilityAuthority(True, "EXACT_SEALED_PROBABILITY_AUTHORIZED", close)


def is_authoritative_sealed_probability(row: Mapping[str, Any]) -> bool:
    return sealed_probability_authority(row).allowed


__all__ = [
    "VERSION", "SEALED_SCOPE", "RECONSTRUCTED_SCOPE",
    "SealedProbabilityAuthority", "sealed_close_value",
    "resolve_confirmed_bar_key", "sealed_probability_authority",
    "is_authoritative_sealed_probability",
]
