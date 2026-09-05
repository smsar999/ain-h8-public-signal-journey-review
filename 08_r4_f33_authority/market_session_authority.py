# -*- coding: utf-8 -*-
"""Market/session authority used by live-source admission.

A4.2.14 Source Transaction Hotfix6 keeps session truth small and explicit:
- Saudi and US regular-session windows are owned by their bar windows.
- Local FX/commodities use a weekly profile rather than civil weekdays.
- Defaults are conservative and may be overridden by the trusted adapter.

The module performs no I/O and introduces no sleeps or polling delays.
"""
from __future__ import annotations

import datetime as dt
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Tuple
from market_key_contract import market_family_long as _canonical_market_family_long

VERSION = "A4_2_14_SOURCE_CLOCK_TRUTH_HOTFIX12H7_SESSION_V2_BASE_HOTFIX12H6"


def market_family(market_key: str) -> str:
    """Return the canonical long-form family from ``market_key_contract``."""
    return _canonical_market_family_long(market_key)


def _parse_hhmm(value: Any, default: dt.time) -> dt.time:
    text = str(value or "").strip()
    if not text:
        return default
    try:
        hh, mm = text.split(":", 1)
        return dt.time(int(hh), int(mm))
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, str(default)) or default).strip())
    except Exception:
        return int(default)


def fx_symbol_family(symbol: str) -> str:
    value = re.sub(r"[^A-Z0-9]", "", str(symbol or "").upper())
    if value.startswith(("XAU", "XAG", "GOLD", "SILVER")):
        return "metals"
    if value in {"CL", "NG", "UKOIL", "USOIL", "WTI", "BRENT", "XBRUSD", "XTIUSD"} or "OIL" in value:
        return "energy"
    if len(value) >= 6 and value[:3].isalpha() and value[3:6].isalpha():
        return "spot_fx"
    return "fx_composite"


@dataclass(frozen=True)
class FxWeeklyProfile:
    profile_id: str
    timezone: str
    weekly_open_weekday: int
    weekly_open_time: dt.time
    weekly_close_weekday: int
    weekly_close_time: dt.time
    daily_breaks: Tuple[Tuple[dt.time, dt.time], ...] = ()


def fx_weekly_profile(symbol: str = "") -> FxWeeklyProfile:
    family = fx_symbol_family(symbol)
    open_weekday = _env_int(f"AIN_FX_{family.upper()}_WEEKLY_OPEN_WEEKDAY", 6)
    close_weekday = _env_int(f"AIN_FX_{family.upper()}_WEEKLY_CLOSE_WEEKDAY", 4)
    open_time = _parse_hhmm(os.getenv(f"AIN_FX_{family.upper()}_WEEKLY_OPEN_HHMM"), dt.time(17, 0))
    close_time = _parse_hhmm(os.getenv(f"AIN_FX_{family.upper()}_WEEKLY_CLOSE_HHMM"), dt.time(17, 0))

    breaks: Tuple[Tuple[dt.time, dt.time], ...] = ()
    raw_breaks = str(os.getenv(f"AIN_FX_{family.upper()}_DAILY_BREAKS", "") or "").strip()
    if raw_breaks:
        parsed = []
        for part in raw_breaks.split(","):
            try:
                left, right = part.split("-", 1)
                parsed.append((_parse_hhmm(left, dt.time(0, 0)), _parse_hhmm(right, dt.time(0, 0))))
            except Exception:
                continue
        breaks = tuple(parsed)

    return FxWeeklyProfile(
        profile_id=f"LOCAL_FX_{family.upper()}_WEEKLY_V1",
        timezone="America/New_York",
        weekly_open_weekday=open_weekday,
        weekly_open_time=open_time,
        weekly_close_weekday=close_weekday,
        weekly_close_time=close_time,
        daily_breaks=breaks,
    )


def _week_minute(value: dt.datetime) -> float:
    sunday_index = (int(value.weekday()) + 1) % 7
    return float(sunday_index * 1440 + value.hour * 60 + value.minute) + value.second / 60.0 + value.microsecond / 60_000_000.0


def _profile_position(weekday: int, value: dt.time) -> float:
    sunday_index = (int(weekday) + 1) % 7
    return float(sunday_index * 1440 + value.hour * 60 + value.minute) + value.second / 60.0


def _inside_daily_break(value: dt.datetime, breaks: Iterable[Tuple[dt.time, dt.time]]) -> bool:
    current = value.time().replace(tzinfo=None)
    for start, end in breaks:
        if start <= end:
            if start <= current < end:
                return True
        else:
            if current >= start or current < end:
                return True
    return False


def fx_market_open(value_ny: dt.datetime, *, symbol: str = "") -> bool:
    profile = fx_weekly_profile(symbol)
    pos = _week_minute(value_ny)
    open_pos = _profile_position(profile.weekly_open_weekday, profile.weekly_open_time)
    close_pos = _profile_position(profile.weekly_close_weekday, profile.weekly_close_time)
    if close_pos <= open_pos:
        inside_week = pos >= open_pos or pos < close_pos
    else:
        inside_week = open_pos <= pos < close_pos
    if not inside_week:
        return False
    if _inside_daily_break(value_ny, profile.daily_breaks):
        return False
    return True


def fx_window_in_session(window_start_ny: dt.datetime, window_end_ny: dt.datetime, *, symbol: str = "") -> bool:
    if window_end_ny <= window_start_ny:
        return False
    epsilon = dt.timedelta(microseconds=1)
    return bool(
        fx_market_open(window_start_ny, symbol=symbol)
        and fx_market_open(window_end_ny - epsilon, symbol=symbol)
    )


def fx_weekly_session_bounds(value_ny: dt.datetime, *, symbol: str = "") -> Tuple[dt.datetime, dt.datetime]:
    value = value_ny.replace(tzinfo=None)
    profile = fx_weekly_profile(symbol)
    days_back = (int(value.weekday()) - int(profile.weekly_open_weekday)) % 7
    open_date = value.date() - dt.timedelta(days=days_back)
    open_dt = dt.datetime.combine(open_date, profile.weekly_open_time)
    if value < open_dt:
        open_dt -= dt.timedelta(days=7)
    close_days = (int(profile.weekly_close_weekday) - int(profile.weekly_open_weekday)) % 7
    close_date = open_dt.date() + dt.timedelta(days=close_days)
    close_dt = dt.datetime.combine(close_date, profile.weekly_close_time)
    if close_dt <= open_dt:
        close_dt += dt.timedelta(days=7)
    return open_dt, close_dt


def fx_next_weekly_open(value_ny: dt.datetime, *, symbol: str = "") -> dt.datetime:
    value = value_ny.replace(tzinfo=None)
    open_dt, close_dt = fx_weekly_session_bounds(value, symbol=symbol)
    if open_dt <= value < close_dt:
        return open_dt
    return open_dt + dt.timedelta(days=7)


def fx_market_closed(value_ny: dt.datetime, *, symbol: str = "", grace_sec: float = 0.0) -> bool:
    value = value_ny.replace(tzinfo=None)
    open_dt, close_dt = fx_weekly_session_bounds(value, symbol=symbol)
    if value < close_dt + dt.timedelta(seconds=max(0.0, float(grace_sec or 0.0))):
        return False
    return value < open_dt + dt.timedelta(days=7)


def fx_session_metadata(symbol: str = "") -> Dict[str, Any]:
    profile = fx_weekly_profile(symbol)
    return {
        "fx_session_profile_id": profile.profile_id,
        "fx_session_timezone": profile.timezone,
        "fx_symbol_family": fx_symbol_family(symbol),
        "fx_weekly_open_weekday": profile.weekly_open_weekday,
        "fx_weekly_open_time": profile.weekly_open_time.isoformat(timespec="minutes"),
        "fx_weekly_close_weekday": profile.weekly_close_weekday,
        "fx_weekly_close_time": profile.weekly_close_time.isoformat(timespec="minutes"),
        "fx_daily_breaks": [
            f"{start.isoformat(timespec='minutes')}-{end.isoformat(timespec='minutes')}"
            for start, end in profile.daily_breaks
        ],
    }


__all__ = [
    "VERSION",
    "market_family",
    "fx_symbol_family",
    "FxWeeklyProfile",
    "fx_weekly_profile",
    "fx_market_open",
    "fx_window_in_session",
    "fx_weekly_session_bounds",
    "fx_next_weekly_open",
    "fx_market_closed",
    "fx_session_metadata",
]
