# -*- coding: utf-8 -*-
"""V86CL R166 — canonical live-sniper event contract.

The trader screen is an event board, not a historical GANN20 scanner:

* LIVE_P30_BORN is created only on the exact source observation that creates the
  positive RSIScaled/VAR cross and already carries the market-threshold P50.
* A born P30 may update inside the same forming bar and, if it seals above the
  threshold, remains on the same exchange-session board.
* If its sealed P50 falls below the threshold, it is removed from the trader
  board and converted into a hidden sealed R1 watch.
* LIVE_R1_BORN is created only by the first future-bar live price observation
  crossing the frozen sealed R1 for an armed below-threshold episode.
* Historical/rebuilt rows and prior-session episodes never create trader events.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Any, Dict, Mapping

VERSION = "V86CL_R166_LIVE_SNIPER_CONTRACT"

LIVE_P30_BORN = "LIVE_P30_BORN"
LIVE_P30_UPDATED = "LIVE_P30_UPDATED"
SEALED_P30_CONFIRMED = "SEALED_P30_CONFIRMED"
SEALED_P30_LATE_CENTER_ONLY = "SEALED_P30_LATE_CENTER_ONLY"
P30_DOWNGRADED_TO_R1_WATCH = "P30_DOWNGRADED_TO_R1_WATCH"
SEALED_R1_WATCH_ARMED = "SEALED_R1_WATCH_ARMED"
LIVE_R1_BORN = "LIVE_R1_BORN"
LIVE_R1_UPDATED = "LIVE_R1_UPDATED"
TRADER_EVENT_CANCELLED = "TRADER_EVENT_CANCELLED"

ORIGIN_LIVE_SOURCE_OBSERVATION = "LIVE_SOURCE_OBSERVATION"
ORIGIN_LIVE_PRICE_TICK = "LIVE_PRICE_TICK"
ORIGIN_SEALED_CROSS_BAR = "SEALED_CROSS_BAR"
ORIGIN_HISTORICAL_REBUILD = "HISTORICAL_REBUILD"

P30_BOARD_EVENTS = {LIVE_P30_BORN, LIVE_P30_UPDATED, SEALED_P30_CONFIRMED}
R1_BOARD_EVENTS = {LIVE_R1_BORN, LIVE_R1_UPDATED}
BOARD_EVENTS = P30_BOARD_EVENTS | R1_BOARD_EVENTS
P30_HIDE_EVENTS = {
    P30_DOWNGRADED_TO_R1_WATCH,
    SEALED_R1_WATCH_ARMED,
    SEALED_P30_LATE_CENTER_ONLY,
    TRADER_EVENT_CANCELLED,
}


def text(value: Any) -> str:
    if value is None:
        return ""
    out = str(value).strip()
    return "" if out.lower() in {"nan", "none", "null", "nat"} else out


def num(value: Any, default: float = float("nan")) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        try:
            return bool(float(value)) and math.isfinite(float(value))
        except Exception:
            return False
    return text(value).lower() in {"1", "true", "yes", "y", "on", "نعم", "صح"}


def date_text(value: Any) -> str:
    raw = text(value).replace("T", " ")
    if len(raw) >= 10 and raw[4:5] == "-" and raw[7:8] == "-":
        return raw[:10]
    try:
        return _dt.datetime.fromisoformat(raw).date().isoformat()
    except Exception:
        return ""


def market_threshold_pct(row: Mapping[str, Any]) -> float:
    r = row or {}
    for key in ("live_pulse_threshold_pct", "fast_cross_min_p50_pct", "immediate_display_min_pct", "threshold_pct"):
        value = num(r.get(key))
        if math.isfinite(value) and value > 0:
            return value
    market = text(r.get("market_key") or r.get("decision_market_key")).lower()
    # H12H14: American display threshold is 40% for both local and API lanes.
    return 40.0 if ("أمريك" in market or "us" in market) else 30.0


def is_review_or_rebuild(row: Mapping[str, Any]) -> bool:
    r = row or {}
    if any(truthy(r.get(key)) for key in (
        "snapshot_review_only", "saved_snapshot_review", "historical_reconstructed",
        "historical_lastbar_review", "r1_activation_reconstructed", "data_stale",
    )):
        return True
    scope = text(r.get("probability_scope") or r.get("gann20_probability_scope") or r.get("truth_scope")).upper()
    if scope in {"SEALED_RECONSTRUCTED", ORIGIN_HISTORICAL_REBUILD}:
        return True
    source = text(r.get("source")).lower()
    return any(token in source for token in ("محفوظ", "تاريخ", "replay", "review", "historical", "reconstructed"))


def exact_same_observation_probability(row: Mapping[str, Any]) -> bool:
    r = row or {}
    if truthy(r.get("probability_at_cross_exact")) or truthy(r.get("probability_exact_at_cross")):
        return True
    if text(r.get("probability_scope_first_cross")).upper() != "LIVE_CURRENT_BAR":
        return False
    source_obs = text(r.get("source_observation_id") or r.get("live_sniper_source_observation_id"))
    probability_obs = text(r.get("probability_source_observation_id") or r.get("live_sniper_probability_source_observation_id"))
    cross_bar = text(r.get("first_cross_bar") or r.get("signal_bar_time"))
    probability_bar = text(r.get("probability_bar_time"))
    return bool(source_obs and probability_obs and source_obs == probability_obs and cross_bar and probability_bar and cross_bar == probability_bar)


def p30_birth_is_valid(row: Mapping[str, Any], session_date: str = "") -> bool:
    r = row or {}
    event = text(r.get("live_sniper_event_type") or r.get("event_type")).upper()
    origin = text(r.get("live_sniper_event_origin") or r.get("event_origin")).upper()
    born_date = date_text(r.get("live_sniper_born_at") or r.get("first_cross_at") or r.get("signal_detected_at"))
    session = date_text(session_date or r.get("trader_board_session_date") or r.get("market_session_date") or r.get("session_data_date"))
    p50 = num(r.get("p50_at_first_cross"), num(r.get("p50_at_cross")))
    explicit_event = event in P30_BOARD_EVENTS or truthy(r.get("live_sniper_p30_birth_proven"))
    return bool(
        explicit_event
        and origin == ORIGIN_LIVE_SOURCE_OBSERVATION
        and session and born_date == session
        and not is_review_or_rebuild(r)
        and exact_same_observation_probability(r)
        and math.isfinite(p50) and p50 >= market_threshold_pct(r)
    )


def r1_birth_is_valid(row: Mapping[str, Any], session_date: str = "") -> bool:
    r = row or {}
    event = text(r.get("live_sniper_event_type") or r.get("event_type")).upper()
    origin = text(r.get("live_sniper_event_origin") or r.get("event_origin")).upper()
    at = r.get("live_sniper_born_at") or r.get("first_r1_crossed_at") or r.get("first_r1_tick_ts") or r.get("gann20_activation_observed_at")
    born_date = date_text(at)
    session = date_text(session_date or r.get("trader_board_session_date") or r.get("market_session_date") or r.get("session_data_date"))
    p50_sealed = num(r.get("p50_sealed"), num(r.get("p50_final")))
    threshold = market_threshold_pct(r)
    explicit_event = event in R1_BOARD_EVENTS or truthy(r.get("live_sniper_r1_birth_proven"))
    return bool(
        explicit_event
        and origin == ORIGIN_LIVE_PRICE_TICK
        and session and born_date == session
        and not is_review_or_rebuild(r)
        and truthy(r.get("r1_watch_armed"))
        and truthy(r.get("r1_future_bar_cross"))
        and math.isfinite(p50_sealed) and 20.0 <= p50_sealed < threshold
    )


def board_birth(row: Mapping[str, Any], session_date: str = "") -> Dict[str, Any]:
    r = row or {}
    if p30_birth_is_valid(r, session_date):
        at = text(r.get("live_sniper_born_at") or r.get("first_cross_at"))
        return {"proven": True, "kind": "P30", "at": at, "date": date_text(at), "reason": "P30 وُلد من تكة التقاطع نفسها واحتماله تجاوز العتبة في الملاحظة ذاتها."}
    if r1_birth_is_valid(r, session_date):
        at = text(r.get("live_sniper_born_at") or r.get("first_r1_crossed_at") or r.get("first_r1_tick_ts"))
        return {"proven": True, "kind": "R1", "at": at, "date": date_text(at), "reason": "R1 اخترق حيًا لأول مرة بعد ختم حلقة أقل من العتبة."}
    return {"proven": False, "kind": "", "at": "", "date": date_text(session_date), "reason": ""}


def p30_should_hide_after_seal(row: Mapping[str, Any]) -> bool:
    r = row or {}
    # H12H14: visibility is threshold-latched.  Once the model reached the
    # market display threshold, later execution/no-chase/seal states may change
    # the status text but must not erase the attention fact.
    if truthy(r.get("attention_visible")) or truthy(r.get("display_threshold_ever_reached")):
        return False
    event = text(r.get("live_sniper_event_type") or r.get("event_type")).upper()
    state = text(r.get("live_pulse_seal_state") or r.get("gann20_episode_state") or r.get("action_state")).upper()
    return event in P30_HIDE_EVENTS or state in {"SEALED_WAITING_R1", "SEALED_REJECTED", "SEALED_P30_LATE_CENTER_ONLY"}


__all__ = [
    "VERSION", "LIVE_P30_BORN", "LIVE_P30_UPDATED", "SEALED_P30_CONFIRMED",
    "SEALED_P30_LATE_CENTER_ONLY", "P30_DOWNGRADED_TO_R1_WATCH",
    "SEALED_R1_WATCH_ARMED", "LIVE_R1_BORN", "LIVE_R1_UPDATED",
    "TRADER_EVENT_CANCELLED", "ORIGIN_LIVE_SOURCE_OBSERVATION",
    "ORIGIN_LIVE_PRICE_TICK", "ORIGIN_SEALED_CROSS_BAR", "ORIGIN_HISTORICAL_REBUILD",
    "P30_BOARD_EVENTS", "R1_BOARD_EVENTS", "BOARD_EVENTS", "P30_HIDE_EVENTS",
    "text", "num", "truthy", "date_text", "market_threshold_pct",
    "is_review_or_rebuild", "exact_same_observation_probability",
    "p30_birth_is_valid", "r1_birth_is_valid", "board_birth",
    "p30_should_hide_after_seal",
]
