# -*- coding: utf-8 -*-
"""Causal freshness guard for live source observations.

A physical DAT file may receive a fresh mtime while its last market bar is from
an old session.  File freshness therefore cannot prove signal freshness.  This
module compares the content bar date with the observation time in the market's
own timezone and fails closed for live-decision work while preserving the row
for audit capture.
"""
from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
import os
from typing import Any, Dict, Mapping, Optional

from market_datetime_normalizer import (
    canonical_market_session_date, market_timezone, parse_datetime,
    source_bar_to_market_naive, to_market_naive,
)
from market_session_authority import fx_session_metadata, fx_window_in_session
from market_key_contract import market_family_long as _canonical_market_family_long, transport_family as _canonical_transport_family
try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

VERSION = "A4_2_14_SOURCE_CLOCK_TRUTH_HOTFIX12H7_FRESHNESS_V2_BASE_HOTFIX12H6"

# A4.2.12: historical/review exemptions are capabilities, not caller-claimable
# booleans.  The opaque object never survives JSON/IPC serialization and can be
# attached only by a trusted in-process replay adapter.
_HISTORICAL_CAPABILITY_KEY = "__ain_trusted_historical_capability__"
_HISTORICAL_CAPABILITY = object()


def grant_historical_replay_capability(
    observation: Mapping[str, Any], *, purpose: str = "trusted_historical_replay",
) -> Dict[str, Any]:
    out = dict(observation or {})
    out[_HISTORICAL_CAPABILITY_KEY] = _HISTORICAL_CAPABILITY
    out["historical_capability_purpose"] = str(purpose or "trusted_historical_replay")
    return out


def has_historical_replay_capability(observation: Mapping[str, Any]) -> bool:
    try:
        return observation.get(_HISTORICAL_CAPABILITY_KEY) is _HISTORICAL_CAPABILITY
    except Exception:
        return False


@dataclass(frozen=True)
class FreshnessDecision:
    allowed: bool
    reason_code: str
    bar_time: str
    observed_at: str
    market_date: str
    bar_date: str
    age_sec: Optional[float] = None
    maximum_lag_sec: Optional[float] = None


def _text(value: Any) -> str:
    text = str(value or "").strip()
    return "" if text.lower() in {"", "none", "nan", "nat", "null"} else text




def _has_explicit_timezone(value: Any) -> bool:
    if isinstance(value, _dt.datetime):
        return value.tzinfo is not None and value.utcoffset() is not None
    text = _text(value).strip()
    if not text:
        return False
    if text.upper().endswith("Z"):
        return True
    tail = text[-6:]
    return bool(len(tail) == 6 and tail[0] in "+-" and tail[1:3].isdigit() and tail[3] == ":" and tail[4:6].isdigit())

def _parse(value: Any) -> Optional[_dt.datetime]:
    text = _text(value).replace("Z", "+00:00")
    if not text:
        return None
    try:
        return _dt.datetime.fromisoformat(text.replace("T", " "))
    except Exception:
        return None


def _zone(market_key: str):
    return market_timezone(market_key)


def _local_date(value: _dt.datetime, zone) -> _dt.date:
    if value.tzinfo is None:
        return value.date()
    return value.astimezone(zone).date()


def _market_family(market_key: str) -> str:
    return _canonical_market_family_long(market_key)


def _maximum_intraday_lag_sec(market_key: str) -> float:
    """Bound old same-session bars without adding sleeps or source I/O.

    FX source labels are writer-clock labels and can legitimately trail the
    canonical New-York session by more than equities during startup.  All limits
    remain configurable for a measured production feed, but fail closed by
    default for multi-hour stale bars.
    """
    family = _market_family(market_key)
    default = {
        "saudi": 90.0 * 60.0,
        "us": 90.0 * 60.0,
        "fx": 4.0 * 60.0 * 60.0,
        "generic": 2.0 * 60.0 * 60.0,
    }[family]
    env_key = f"AIN_LIVE_SOURCE_MAX_LAG_SEC_{family.upper()}"
    try:
        return max(30.0 * 60.0, float(os.getenv(env_key, str(default)) or default))
    except Exception:
        return default


def _timeframe_minutes(value: Any) -> int:
    text = str(value or "30M").strip().upper().replace("MIN", "M")
    text = {"M30": "30M", "30": "30M", "H1": "1H", "60M": "1H"}.get(text, text)
    if text.endswith("M") and text[:-1].isdigit():
        return max(1, int(text[:-1]))
    if text.endswith("H") and text[:-1].isdigit():
        return max(1, int(text[:-1]) * 60)
    return 30


def assess_live_source_bar(
    observation: Mapping[str, Any], *, market_key: str,
    now: Optional[_dt.datetime] = None,
) -> FreshnessDecision:
    source = observation or {}
    bar_text = _text(source.get("bar_datetime") or source.get("data_datetime") or source.get("bar_time"))
    # ``observed_at`` is a legacy/publication wall-clock field whose serializer
    # intentionally strips tzinfo.  It must never be treated as proof of elapsed
    # live time.  H12H6 carries a separate aware receipt instant for that purpose.
    observed_public_value = source.get("observed_at") or source.get("source_observed_at")
    receipt_value = (
        source.get("source_received_at_utc")
        or source.get("source_observed_at_aware")  # compatibility with external audit fixture
        or observed_public_value
    )
    observed_text = _text(observed_public_value or receipt_value)
    receipt_text = _text(receipt_value)
    if has_historical_replay_capability(source):
        return FreshnessDecision(True, "TRUSTED_HISTORICAL_CAPABILITY_EXEMPT", bar_text, observed_text, "", "")
    bar_dt = parse_datetime(bar_text)
    try:
        zone = _zone(market_key)
        receipt_dt = parse_datetime(receipt_text) or now or _dt.datetime.now(_dt.timezone.utc)
        market_date = _local_date(receipt_dt, zone)
    except Exception as exc:
        return FreshnessDecision(
            False,
            f"LIVE_SOURCE_TIMEZONE_INVALID:{type(exc).__name__}",
            bar_text, observed_text, "", "",
        )
    if bar_dt is None:
        return FreshnessDecision(False, "LIVE_SOURCE_BAR_TIME_MISSING", bar_text, observed_text, market_date.isoformat(), "")
    try:
        bar_market = source_bar_to_market_naive(
            bar_text, market_key=market_key, observed_at=(receipt_text or None),
        )
        observed_market = to_market_naive(receipt_dt, market_key=market_key)
    except Exception as exc:
        return FreshnessDecision(
            False,
            f"LIVE_SOURCE_SESSION_NORMALIZATION_FAILED:{type(exc).__name__}",
            bar_text, observed_text, market_date.isoformat(), "",
        )
    if bar_market is None:
        return FreshnessDecision(False, "LIVE_SOURCE_BAR_TIME_MISSING", bar_text, observed_text, market_date.isoformat(), "")

    family = _market_family(market_key)
    timeframe_minutes = _timeframe_minutes(source.get("timeframe") or "30M")
    label_mode = str(source.get("bar_label_mode") or "").strip().lower()
    if not label_mode and _canonical_transport_family(market_key) == "local" and family in {"saudi", "us", "fx"}:
        label_mode = "end"
    if label_mode == "end":
        window_start = bar_market - _dt.timedelta(minutes=timeframe_minutes)
        window_end = bar_market
    else:
        window_start = bar_market
        window_end = bar_market + _dt.timedelta(minutes=timeframe_minutes)

    if family == "fx":
        # Weekly ownership follows the canonical New-York bar window, not civil
        # Monday–Friday dates or the end-label date. This accepts 23:30→00:00 and
        # Sunday open, while rejecting Friday after weekly close.
        bar_date = window_start.date()
        symbol = str(source.get("symbol") or "").strip().upper()
        try:
            if not fx_window_in_session(window_start, window_end, symbol=symbol):
                return FreshnessDecision(
                    False, "LIVE_SOURCE_BAR_OUTSIDE_FX_WEEKLY_SESSION",
                    bar_text, observed_text, market_date.isoformat(), bar_date.isoformat(),
                )
        except Exception as exc:
            return FreshnessDecision(
                False, f"LIVE_SOURCE_FX_SESSION_AUTHORITY_FAILED:{type(exc).__name__}",
                bar_text, observed_text, market_date.isoformat(), bar_date.isoformat(),
            )
        if observed_market is not None:
            future_sec = float((window_start - observed_market).total_seconds())
            if future_sec > float(timeframe_minutes * 60):
                return FreshnessDecision(
                    False, "LIVE_SOURCE_BAR_FROM_FUTURE_SESSION",
                    bar_text, observed_text, market_date.isoformat(), bar_date.isoformat(),
                )
    else:
        bar_date = window_start.date() if label_mode == "end" else bar_market.date()
        if bar_date < market_date:
            return FreshnessDecision(False, "LIVE_SOURCE_BAR_FROM_OLD_SESSION", bar_text, observed_text, market_date.isoformat(), bar_date.isoformat())
        if bar_date > market_date:
            return FreshnessDecision(False, "LIVE_SOURCE_BAR_FROM_FUTURE_SESSION", bar_text, observed_text, market_date.isoformat(), bar_date.isoformat())
    maximum_lag = _maximum_intraday_lag_sec(market_key)
    age_sec: Optional[float] = None
    # Intraday age is an official-live gate only when the observation clock is
    # explicit.  A naive timestamp cannot prove elapsed market time across source
    # adapters; session-date freshness remains enforced, while production source
    # observations are timezone-aware and receive the full age gate.
    explicit_observation_clock = bool(
        _has_explicit_timezone(receipt_value)
        or (not receipt_text and now is not None and _has_explicit_timezone(now))
    )
    if explicit_observation_clock and bar_market is not None and observed_market is not None:
        age_sec = float((observed_market - bar_market).total_seconds())
        if age_sec > maximum_lag:
            return FreshnessDecision(
                False, "LIVE_SOURCE_BAR_INTRADAY_STALE",
                bar_text, observed_text, market_date.isoformat(), bar_date.isoformat(),
                age_sec=age_sec, maximum_lag_sec=maximum_lag,
            )
    return FreshnessDecision(
        True, "LIVE_SOURCE_BAR_CURRENT_SESSION",
        bar_text, observed_text, market_date.isoformat(), bar_date.isoformat(),
        age_sec=age_sec, maximum_lag_sec=maximum_lag,
    )


__all__ = [
    "VERSION", "FreshnessDecision", "assess_live_source_bar",
    "grant_historical_replay_capability", "has_historical_replay_capability",
]
