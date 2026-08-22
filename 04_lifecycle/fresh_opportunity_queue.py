# -*- coding: utf-8 -*-
"""V86CL R166 — live sniper trader-board classifier.

The trader board contains only two kinds of events born from live source truth:

1. P30 born on the exact RSIScaled/VAR crossing observation with an exact
   same-observation probability already above the market threshold.
2. The first live future-bar crossing of sealed frozen R1 for a below-threshold
   episode.

A valid event remains on the board for its exchange session.  P30 is the one
exception: if its final sealed probability falls below the threshold, it is
removed immediately and converted into a hidden R1 watch.  History, rebuilt
rows, previous-session episodes and model outcomes stay in Portfolio Center.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Iterable, List, Mapping, Optional

from live_episode_truth_contract import TRUTH_PREVIEW
from trader_queue_truth_contract import enforce as _enforce_trader_truth
from live_sniper_contract import (
    VERSION as SNIPER_CONTRACT_VERSION,
    LIVE_P30_BORN, LIVE_P30_UPDATED, SEALED_P30_CONFIRMED,
    P30_DOWNGRADED_TO_R1_WATCH, SEALED_R1_WATCH_ARMED,
    LIVE_R1_BORN, LIVE_R1_UPDATED,
    P30_BOARD_EVENTS, R1_BOARD_EVENTS,
    board_birth, date_text, is_review_or_rebuild, market_threshold_pct,
    num as _num, p30_should_hide_after_seal, text as _text, truthy as _truthy,
)

VERSION = "V86CL_R169_3_6_9_EXECUTION_AUTHORITY_TRADER_BOARD"

BUCKET_ENTRY_NOW = "ENTRY_NOW"
# Keep persisted schema value for compatibility with older UI/manifests.
BUCKET_SESSION_SIGNAL = "FRESH_RADAR"
BUCKET_LIVE_PULSE = BUCKET_SESSION_SIGNAL
BUCKET_FRESH_RADAR = BUCKET_SESSION_SIGNAL
BUCKET_PORTFOLIO_ONLY = "PORTFOLIO_ONLY"
BUCKET_AUDIT_ONLY = "AUDIT_ONLY"

DEFAULT_FRESH_RADAR_MAX_EVENT_AGE_BARS = 20  # compatibility only
DEFAULT_ENTRY_MAX_EVENT_AGE_BARS = 1
_CLOSED_OUTCOMES = {"R50_HIT_TRACKING_R100", "R100_HIT", "EXPIRED_20_BARS"}


def _structured_outcome(row: Mapping[str, Any]) -> str:
    return _text((row or {}).get("gann20_episode_outcome")).upper()


def is_review_or_audit_row(row: Mapping[str, Any]) -> bool:
    return is_review_or_rebuild(row)


def event_kind(row: Mapping[str, Any]) -> str:
    r = row or {}
    event = _text(r.get("live_sniper_event_type") or r.get("event_type")).upper()
    if event in R1_BOARD_EVENTS:
        return "R1"
    if event in P30_BOARD_EVENTS:
        return "P30"
    if _truthy(r.get("execution_authorized")) and (_truthy(r.get("execution_passed")) or _truthy(r.get("execution_publishable"))):
        return "EXECUTION"
    return "OTHER"


def event_time_text(row: Mapping[str, Any]) -> str:
    r = row or {}
    if event_kind(r) == "R1":
        keys = ("live_sniper_born_at", "first_r1_crossed_at", "first_r1_tick_ts", "gann20_activation_observed_at")
    else:
        keys = ("live_sniper_born_at", "first_cross_at", "signal_detected_at", "appearance_at")
    for key in keys:
        value = _text(r.get(key))
        if value:
            return value
    return ""


def market_session_date(row: Mapping[str, Any], session_data_date: str = "") -> str:
    r = row or {}
    for value in (
        session_data_date, r.get("trader_board_session_date"), r.get("market_session_date"),
        r.get("session_data_date"), r.get("latest_market_bar_time"), r.get("latest_bar_time"),
    ):
        out = date_text(value)
        if out:
            return out
    return ""


def session_signal_birth(row: Mapping[str, Any], *, session_data_date: str = "") -> Dict[str, Any]:
    return board_birth(row, market_session_date(row, session_data_date=session_data_date))


live_pulse_birth = session_signal_birth


def event_age_bars(row: Mapping[str, Any]) -> Optional[int]:
    r = row or {}
    elapsed = _num(r.get("model_bars_elapsed"), _num(r.get("gann20_episode_effective_age_bars")))
    if not math.isfinite(elapsed):
        elapsed = _num(r.get("gann20_episode_age_bars"))
    if event_kind(r) == "R1":
        hit_bar = _num(r.get("gann20_r1_first_hit_bar"), _num(r.get("r1_activation_age_bars")))
        if math.isfinite(elapsed) and math.isfinite(hit_bar):
            return max(0, int(elapsed) - int(hit_bar))
    if math.isfinite(elapsed):
        return max(0, int(elapsed))
    return 0 if event_time_text(r) else None


def target_consumed_truth(row: Mapping[str, Any]) -> bool:
    r = row or {}
    outcome = _structured_outcome(r)
    if outcome in {"R50_HIT_TRACKING_R100", "R100_HIT"}:
        return True
    if _truthy(r.get("gann20_r50_hit")) or _truthy(r.get("gann20_target_consumed")):
        return True
    consumed = _num(r.get("execution_consumed_to_r50"))
    if math.isfinite(consumed) and consumed >= 0.999:
        return True
    return any(_truthy(r.get(k)) for k in (
        "target_consumed_before_entry", "target_reached_before_official", "target_consumed_before_official",
    ))


def entry_window_closed(row: Mapping[str, Any]) -> bool:
    r = row or {}
    return bool(
        target_consumed_truth(r)
        or _truthy(r.get("gann20_entry_opportunity_closed"))
        or _truthy(r.get("gann20_episode_terminal"))
        or _structured_outcome(r) in _CLOSED_OUTCOMES
    )


def risk_or_no_chase_blocked(row: Mapping[str, Any]) -> bool:
    r = row or {}
    decision = _text(r.get("execution_decision")).upper()
    if decision in {"BLOCKED_HARD_RISK", "NO_CHASE", "ENTRY_EXPIRED", "REVIEW_ONLY"}:
        return True
    blob = " | ".join(_text(r.get(k)) for k in (
        "execution_decision", "execution_blockers", "entry_status", "entry_status_ar",
        "monitor_state_ar", "monitor_outcome_ar", "visible_signal_case_ar", "radar_stage",
    ))
    return any(token in blob for token in ("لا تطارد", "محظورة مخاطرة", "غير قابلة للتنفيذ", "انتهت صلاحية الدخول"))


def execution_qualified(row: Mapping[str, Any]) -> bool:
    r = row or {}
    return bool(
        _truthy(r.get("execution_authorized"))
        and (
            _truthy(r.get("execution_passed"))
            or _truthy(r.get("execution_publishable"))
            or _text(r.get("execution_decision")).upper() == "EXECUTION_AUTHORIZED"
        )
    )


def same_market_session_event(row: Mapping[str, Any], *, session_data_date: str = "") -> bool:
    return bool(session_signal_birth(row, session_data_date=session_data_date).get("proven"))


def _persisted_valid_birth(row: Mapping[str, Any], session_date: str) -> Dict[str, Any]:
    """Use ledger proof only when it contains explicit R166 provenance.

    A generic prior `born_today` flag is intentionally insufficient; that was the
    source of historical rows being relabelled as live pulses in R165.1.
    """
    r = row or {}
    proven = _truthy(r.get("live_sniper_birth_proven"))
    born_date = date_text(r.get("trader_board_session_date") or r.get("live_sniper_session_date"))
    kind = _text(r.get("live_sniper_birth_kind")).upper()
    if proven and born_date and born_date == session_date and kind in {"P30", "R1"}:
        return {
            "proven": True,
            "kind": kind,
            "at": _text(r.get("live_sniper_born_at")),
            "date": born_date,
            "reason": _text(r.get("live_sniper_birth_reason_ar")),
        }
    return {"proven": False, "kind": "", "at": "", "date": session_date, "reason": ""}


def _attention_score(row: Mapping[str, Any], bucket: str, age: Optional[int]) -> float:
    rr = _num(row.get("execution_reward_risk_to_r50"), 0.0)
    room = _num(row.get("execution_room_to_r50_pct"), 0.0)
    p50 = _num(row.get("gann20_p_r50_pct"), _num(row.get("p50_live"), 0.0))
    base = 1000.0 if bucket == BUCKET_ENTRY_NOW else 500.0 if bucket == BUCKET_SESSION_SIGNAL else 0.0
    return round(base + max(-2.0, min(8.0, rr)) * 25.0 + max(-10.0, min(10.0, room)) * 4.0 + p50 * 0.5 - float(age or 0) * 5.0, 3)


def classify_row(
    row: Mapping[str, Any], *, session_data_date: str = "",
    fresh_radar_max_event_age_bars: int = DEFAULT_FRESH_RADAR_MAX_EVENT_AGE_BARS,
    entry_max_event_age_bars: int = DEFAULT_ENTRY_MAX_EVENT_AGE_BARS,
) -> Dict[str, Any]:
    del fresh_radar_max_event_age_bars
    r = dict(row or {})
    session_date = market_session_date(r, session_data_date=session_data_date)
    birth = session_signal_birth(r, session_data_date=session_date)
    if not birth.get("proven"):
        birth = _persisted_valid_birth(r, session_date)

    review = is_review_or_audit_row(r)
    born_today = bool(birth.get("proven")) and date_text(birth.get("date")) == session_date
    # H12H14 separates immutable birth provenance from attention visibility.
    # A background episode can become visible later in the same bar or at seal
    # once the model threshold is reached, without rewriting its first-cross score.
    attention_at = _text(
        r.get("display_threshold_first_reached_at")
        or r.get("attention_eligible_at")
        or r.get("first_visible_at")
        or r.get("appearance_at")
    )
    attention_today = bool(
        (_truthy(r.get("attention_visible")) or _truthy(r.get("display_threshold_ever_reached")))
        and attention_at and date_text(attention_at) == session_date
    )
    p30_hidden = bool(p30_should_hide_after_seal(r) and not attention_today)
    board_visible = bool((born_today or attention_today) and not p30_hidden)
    qualified = execution_qualified(r)
    closed = entry_window_closed(r)
    blocked = risk_or_no_chase_blocked(r)
    age = event_age_bars(r)

    if review and not board_visible:
        bucket = BUCKET_AUDIT_ONLY
        reason = "صف تاريخي أو معاد البناء؛ محفوظ للتدقيق ولا يولد إشارة قنص."
    elif board_visible and qualified and not blocked and not closed and (age is None or age <= int(entry_max_event_age_bars)):
        bucket = BUCKET_ENTRY_NOW
        reason = "حدث حي حقيقي اجتاز طبقة التنفيذ وما زالت نافذة الدخول مفتوحة."
    elif board_visible:
        bucket = BUCKET_SESSION_SIGNAL
        if birth.get("kind") == "R1":
            reason = "اختراق R1 حي ظهر لأول مرة اليوم وسيبقى حتى بداية الجلسة الجديدة."
        elif attention_today and not born_today:
            reason = "بلغ النموذج عتبة الظهور اليوم بعد أول تقاطع أو عند الختم؛ بقيت حقيقة أول تقاطع محفوظة دون إعادة كتابة."
        else:
            reason = "بلغ النموذج عتبة الظهور اليوم وستبقى الحلقة في لوحة الجلسة؛ حالة التنفيذ لا تلغي حقيقة الظهور."
    else:
        bucket = BUCKET_PORTFOLIO_ONLY
        reason = (
            "P30 خُفّضت عند الختم إلى مراقبة R1 سرية؛ لا تعود للوحة إلا باختراق R1 حي جديد."
            if p30_hidden
            else "حلقة نموذج أو نتيجة أو حدث غير مثبت حيًا؛ محفوظ في مركز المنظومات."
        )

    label = {
        BUCKET_ENTRY_NOW: "جاهزة للدخول",
        BUCKET_SESSION_SIGNAL: "إشارة جلسة اليوم",
        BUCKET_PORTFOLIO_ONLY: "مركز المنظومات",
        BUCKET_AUDIT_ONLY: "تدقيق فقط",
    }[bucket]
    score = _attention_score(r, bucket, age)
    rank = {BUCKET_ENTRY_NOW: 0, BUCKET_SESSION_SIGNAL: 1, BUCKET_PORTFOLIO_ONLY: 2, BUCKET_AUDIT_ONLY: 3}[bucket]
    priority = rank * 1_000_000 + int(age or 0) * 10_000 - int(score * 10)
    return {
        "trader_queue_bucket": bucket,
        "trader_queue_bucket_ar": label,
        "trader_queue_reason_ar": reason,
        "trader_queue_eligible": bucket in {BUCKET_ENTRY_NOW, BUCKET_SESSION_SIGNAL},
        "trader_session_keep_until_new_day": bool(board_visible),
        "threshold_attention_proven": bool(attention_today),
        "threshold_attention_at": attention_at,
        "live_sniper_birth_proven": bool(born_today),
        "live_sniper_birth_kind": _text(birth.get("kind")),
        "live_sniper_born_at": _text(birth.get("at")),
        "live_sniper_session_date": _text(birth.get("date")),
        "live_sniper_birth_reason_ar": _text(birth.get("reason")),
        # Compatibility fields consumed by existing UI/reports.
        "session_signal_birth_proven": bool(born_today),
        "live_pulse_birth_proven": bool(born_today and birth.get("kind") == "P30"),
        "session_signal_birth_kind": _text(birth.get("kind")),
        "live_pulse_kind": _text(birth.get("kind")),
        "session_signal_birth_at": _text(birth.get("at")),
        "live_pulse_born_at": _text(birth.get("at")),
        "session_signal_date": _text(birth.get("date")),
        "live_pulse_session_date": _text(birth.get("date")),
        "session_signal_birth_reason_ar": _text(birth.get("reason")),
        "session_signal_terminal": bool(closed),
        "fresh_event_kind": _text(birth.get("kind")) or event_kind(r),
        "fresh_event_at": _text(birth.get("at")) or event_time_text(r),
        "fresh_event_age_bars": age,
        "fresh_event_same_session": bool(born_today),
        "target_consumed_truth": bool(target_consumed_truth(r)),
        "entry_window_closed": bool(closed),
        "p30_hidden_after_seal": bool(p30_hidden),
        "trader_attention_score": score,
        "trader_queue_sort_priority": priority,
        "fresh_opportunity_policy_version": VERSION,
        "live_sniper_contract_version": SNIPER_CONTRACT_VERSION,
    }


def _repair_display(row: Dict[str, Any]) -> None:
    bucket = _text(row.get("trader_queue_bucket")).upper()
    kind = _text(row.get("live_sniper_birth_kind") or row.get("session_signal_birth_kind")).upper()
    if bucket not in {BUCKET_ENTRY_NOW, BUCKET_SESSION_SIGNAL}:
        return
    if kind == "R1":
        row["visible_signal_case_ar"] = "اختراق R1 حي"
        row["radar_stage"] = "اختراق R1 حي — جلسة اليوم"
        row["status"] = "اختراق R1 حي"
        row["signal_entry_price"] = row.get("first_r1_price") or row.get("r1_activation_price") or row.get("appearance_price")
    else:
        row["visible_signal_case_ar"] = "نبضة P30 حية"
        row["radar_stage"] = "تقاطع حي — شمعة النبضة"
        row["status"] = "نبضة P30 حية"
        row["signal_entry_price"] = row.get("first_cross_price") or row.get("appearance_price")
    row["entry_signal_price"] = row.get("signal_entry_price")
    if execution_qualified(row) and not risk_or_no_chase_blocked(row):
        row["entry_status"] = "EXECUTION_QUALIFIED"
        row["entry_status_ar"] = "مؤهلة للتنفيذ"
    else:
        row["entry_status"] = "RADAR_ONLY"
        row["entry_status_ar"] = "رادار فقط — لا شراء"




def _enforce_trader_projection(row: Mapping[str, Any]) -> Dict[str, Any]:
    out, valid, reason = _enforce_trader_truth(
        row, truth_source=str((row or {}).get("truth_source") or TRUTH_PREVIEW), strict=True,
    )
    if valid:
        _repair_display(out)
        return out
    out.update({
        "trader_queue_bucket": BUCKET_AUDIT_ONLY,
        "trader_queue_bucket_ar": "تدقيق فقط",
        "trader_queue_reason_ar": f"حُجر الصف لغياب عقد هوية/حقيقة مكتمل: {reason}",
        "trader_queue_eligible": False,
        "trader_session_keep_until_new_day": False,
        "trader_queue_sort_priority": 3_999_999_999,
    })
    return out


def stamp_row(
    row: Mapping[str, Any], *, session_data_date: str = "",
    fresh_radar_max_event_age_bars: int = DEFAULT_FRESH_RADAR_MAX_EVENT_AGE_BARS,
    entry_max_event_age_bars: int = DEFAULT_ENTRY_MAX_EVENT_AGE_BARS,
) -> Dict[str, Any]:
    out = dict(row or {})
    out.update(classify_row(
        out, session_data_date=session_data_date,
        fresh_radar_max_event_age_bars=fresh_radar_max_event_age_bars,
        entry_max_event_age_bars=entry_max_event_age_bars,
    ))
    out = _enforce_trader_projection(out)
    eligible = bool(out.get("trader_queue_eligible"))
    out["current_monitor_eligible"] = eligible
    out["_exclude_from_current_monitor"] = not eligible
    if not eligible:
        out["portfolio_followup_eligible"] = True
    return out


def stamp_rows(
    rows: Iterable[Mapping[str, Any]], *, session_data_date: str = "",
    fresh_radar_max_event_age_bars: int = DEFAULT_FRESH_RADAR_MAX_EVENT_AGE_BARS,
    entry_max_event_age_bars: int = DEFAULT_ENTRY_MAX_EVENT_AGE_BARS,
) -> List[Dict[str, Any]]:
    return [stamp_row(
        row, session_data_date=session_data_date,
        fresh_radar_max_event_age_bars=fresh_radar_max_event_age_bars,
        entry_max_event_age_bars=entry_max_event_age_bars,
    ) for row in (rows or [])]


def queue_counts(rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    counts = {BUCKET_ENTRY_NOW: 0, BUCKET_SESSION_SIGNAL: 0, BUCKET_PORTFOLIO_ONLY: 0, BUCKET_AUDIT_ONLY: 0}
    for row in rows or []:
        bucket = _text((row or {}).get("trader_queue_bucket")).upper()
        if bucket in counts:
            counts[bucket] += 1
    counts["FRESH_RADAR"] = counts[BUCKET_SESSION_SIGNAL]
    counts["LIVE_PULSE"] = sum(1 for row in rows or [] if _text((row or {}).get("live_sniper_birth_kind")).upper() == "P30" and _text((row or {}).get("trader_queue_bucket")).upper() in {BUCKET_ENTRY_NOW, BUCKET_SESSION_SIGNAL})
    counts["LIVE_R1"] = sum(1 for row in rows or [] if _text((row or {}).get("live_sniper_birth_kind")).upper() == "R1" and _text((row or {}).get("trader_queue_bucket")).upper() in {BUCKET_ENTRY_NOW, BUCKET_SESSION_SIGNAL})
    counts["TRADER_VISIBLE"] = counts[BUCKET_ENTRY_NOW] + counts[BUCKET_SESSION_SIGNAL]
    counts["CENTER_TOTAL"] = counts[BUCKET_PORTFOLIO_ONLY] + counts[BUCKET_AUDIT_ONLY]
    return counts


def sort_trader_queue(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return sorted([dict(r or {}) for r in rows or []], key=lambda r: (
        int(_num(r.get("trader_queue_sort_priority"), 9_999_999_999)),
        -_num(r.get("trader_attention_score"), 0.0),
        _text(r.get("symbol")),
    ))


__all__ = [
    "VERSION", "BUCKET_ENTRY_NOW", "BUCKET_LIVE_PULSE", "BUCKET_SESSION_SIGNAL", "BUCKET_FRESH_RADAR",
    "BUCKET_PORTFOLIO_ONLY", "BUCKET_AUDIT_ONLY", "event_kind", "event_time_text",
    "session_signal_birth", "live_pulse_birth", "event_age_bars", "target_consumed_truth", "entry_window_closed",
    "risk_or_no_chase_blocked", "execution_qualified", "same_market_session_event",
    "classify_row", "stamp_row", "stamp_rows", "queue_counts", "sort_trader_queue",
]
