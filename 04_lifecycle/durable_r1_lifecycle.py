# -*- coding: utf-8 -*-
"""Single durable state machine for post-seal R1 lifecycle truth.

The state belongs to the episode, not to one emitted row.  It survives every
subsequent tick and is stamped with R1 authority after activation so a sealed
watch row can never regress it.
"""
from __future__ import annotations

from typing import Any, Mapping, MutableMapping

from live_episode_truth_contract import TRUTH_R1_ACTIVATION, TRUTH_TERMINAL

VERSION = "A104_DURABLE_STOPPED_OUT_LIFECYCLE_V1"

LIVE_PENDING_SEAL = "LIVE_PENDING_SEAL"
LIVE_WEAKENING = "LIVE_WEAKENING"
SEAL_AWAITING_FINAL_SCORE = "SEAL_AWAITING_FINAL_SCORE"
SEALED_P30_CONFIRMED = "SEALED_P30_CONFIRMED"
SEALED_WAITING_R1 = "SEALED_WAITING_R1"
LIVE_WAITING_R1 = "LIVE_WAITING_R1"
R1_ACTIVE_WAITING_R50 = "R1_ACTIVE_WAITING_R50"
R1_LOST_WAITING_REGAIN = "R1_LOST_WAITING_REGAIN"
R50_HIT_TRACKING_R100 = "R50_HIT_TRACKING_R100"
R100_HIT_COMPLETE = "R100_HIT_COMPLETE"
PRE_ENTRY_STOP_INVALIDATED = "PRE_ENTRY_STOP_INVALIDATED"
EPISODE_EXPIRED_20_BARS = "EPISODE_EXPIRED_20_BARS"
EPISODE_CLOSED_NEGATIVE_CROSS = "EPISODE_CLOSED_NEGATIVE_CROSS"
EXPIRED_SESSION_CLOSE = "EXPIRED_SESSION_CLOSE"
STOPPED_OUT = "STOPPED_OUT"

POST_R1_ACTIVE_STATES = {
    R1_ACTIVE_WAITING_R50,
    R1_LOST_WAITING_REGAIN,
    R50_HIT_TRACKING_R100,
}
PERSISTABLE_ACTIVE_STATES = {
    LIVE_PENDING_SEAL,
    LIVE_WEAKENING,
    SEAL_AWAITING_FINAL_SCORE,
    SEALED_P30_CONFIRMED,
    SEALED_WAITING_R1,
    LIVE_WAITING_R1,
    *POST_R1_ACTIVE_STATES,
}
TERMINAL_STATES = {
    R100_HIT_COMPLETE,
    PRE_ENTRY_STOP_INVALIDATED,
    EPISODE_EXPIRED_20_BARS,
    EPISODE_CLOSED_NEGATIVE_CROSS,
    EXPIRED_SESSION_CLOSE,
    STOPPED_OUT,
}


def is_terminal(candidate: Mapping[str, Any]) -> bool:
    state = str((candidate or {}).get("episode_state") or "").upper()
    return state in TERMINAL_STATES


def is_post_r1(candidate: Mapping[str, Any]) -> bool:
    state = str((candidate or {}).get("episode_state") or "").upper()
    return bool(
        not is_terminal(candidate)
        and ((candidate or {}).get("activated_r1") or state in POST_R1_ACTIVE_STATES)
    )


def _state_from_flags(candidate: Mapping[str, Any]) -> str:
    source = candidate or {}
    if source.get("pre_entry_stop_invalidated"):
        return PRE_ENTRY_STOP_INVALIDATED
    if source.get("stopped_out") and source.get("activated_r1"):
        return STOPPED_OUT
    if source.get("model_episode_closed_by_negative_cross"):
        return EPISODE_CLOSED_NEGATIVE_CROSS
    if source.get("model_horizon_expired"):
        return EPISODE_EXPIRED_20_BARS
    if source.get("hit_r100"):
        return R100_HIT_COMPLETE
    if source.get("hit_r50"):
        return R50_HIT_TRACKING_R100
    if source.get("activated_r1"):
        return R1_LOST_WAITING_REGAIN if source.get("r1_currently_lost") else R1_ACTIVE_WAITING_R50
    return str(source.get("episode_state") or SEALED_WAITING_R1)


def apply_transition(
    candidate: MutableMapping[str, Any], *, crossed_r1: bool = False,
    lost_r1: bool = False, regained_r1: bool = False,
    hit_r50: bool = False, hit_r100: bool = False, hit_stop: bool = False,
) -> str:
    """Update the episode-owned state after one price observation."""
    if crossed_r1:
        candidate["activated_r1"] = True
        candidate["r1_currently_lost"] = False
    if lost_r1:
        candidate["lost_r1"] = True
        candidate["r1_currently_lost"] = True
    if regained_r1:
        candidate["regained_r1"] = True
        candidate["r1_currently_lost"] = False
    if hit_r50:
        candidate["hit_r50"] = True
    if hit_r100:
        candidate["hit_r100"] = True
    if hit_stop and candidate.get("activated_r1"):
        candidate["hit_stop"] = True
        candidate["stopped_out"] = True
    state = _state_from_flags(candidate)
    candidate["episode_state"] = state
    candidate["gann20_episode_state"] = state
    candidate["live_pulse_seal_state"] = state
    candidate["action_state"] = state
    if state in TERMINAL_STATES:
        candidate["truth_source"] = TRUTH_TERMINAL
        candidate["truth_rank"] = 60
        candidate["live_publishable"] = False
        candidate["r1_watch_armed"] = False
        candidate["r1_watch_mode"] = None
        candidate["r1_currently_lost"] = False
        candidate["terminal_state"] = state
    elif is_post_r1(candidate):
        candidate["truth_source"] = TRUTH_R1_ACTIVATION
        candidate["truth_rank"] = 55
        candidate["live_publishable"] = True
        candidate["published_to_trader"] = True
    return state


def apply_terminal_state(candidate: MutableMapping[str, Any], state: str, *, reason: str = "", at: Any = None) -> str:
    """Apply one authoritative terminal state to the durable candidate."""
    target = str(state or "").upper()
    if target not in TERMINAL_STATES:
        raise ValueError(f"UNSUPPORTED_TERMINAL_STATE:{target}")
    candidate["episode_state"] = target
    candidate["gann20_episode_state"] = target
    candidate["live_pulse_seal_state"] = target
    candidate["action_state"] = target
    candidate["truth_source"] = TRUTH_TERMINAL
    candidate["truth_rank"] = 60
    candidate["live_publishable"] = False
    candidate["published_to_trader"] = bool(candidate.get("published_to_trader"))
    candidate["r1_watch_armed"] = False
    candidate["r1_watch_mode"] = None
    candidate["r1_currently_lost"] = False
    candidate["terminal_state"] = target
    candidate["terminal_reason"] = str(reason or target)
    if at is not None:
        candidate["terminal_at"] = str(at)
    return target


def presentation(candidate: Mapping[str, Any]) -> dict[str, Any]:
    state = str((candidate or {}).get("episode_state") or "")
    mapping = {
        SEALED_WAITING_R1: ("مراقبة R1 — مستويات الختم مجمدة", "ينتظر R1", False),
        LIVE_WAITING_R1: ("مراقبة R1 حية — احتمال الميلاد 20–30", "ينتظر R1", False),
        R1_ACTIVE_WAITING_R50: ("اختراق R1 حي — ينتظر R50", "تحقق R1", True),
        R1_LOST_WAITING_REGAIN: ("فقد R1 — ينتظر الاستعادة", "فقد R1 — تحت المتابعة", True),
        R50_HIT_TRACKING_R100: ("بلغ R50 — ينتظر R100", "بلغ R50", True),
        R100_HIT_COMPLETE: ("اكتملت الحلقة — تحقق R100", "تحقق R100", False),
        PRE_ENTRY_STOP_INVALIDATED: ("انتهت — ضُرب الوقف قبل الدخول", "أُبطلت قبل الدخول", False),
        EPISODE_EXPIRED_20_BARS: ("انتهت مهلة الحلقة بعد 20 شمعة", "انتهى الأفق", False),
        EPISODE_CLOSED_NEGATIVE_CROSS: ("انتهت الحلقة بتقاطع سلبي", "تقاطع سلبي", False),
        EXPIRED_SESSION_CLOSE: ("انتهت الجلسة — أغلقت المتابعة", "انتهاء الجلسة", False),
        STOPPED_OUT: ("انتهت الصفقة — تحقق الوقف بعد R1", "ضُرب الوقف — خروج", False),
    }
    stage, status, publishable = mapping.get(
        state,
        (str((candidate or {}).get("radar_stage") or ""), str((candidate or {}).get("status") or ""), bool((candidate or {}).get("live_publishable"))),
    )
    return {
        "gann20_episode_state": state,
        "live_pulse_seal_state": state,
        "action_state": state,
        "radar_stage": stage,
        "status": status,
        "live_publishable": bool(publishable),
        "published_to_trader": bool((candidate or {}).get("published_to_trader") or is_post_r1(candidate)),
    }


__all__ = [
    "VERSION", "SEALED_WAITING_R1", "LIVE_WAITING_R1",
    "R1_ACTIVE_WAITING_R50", "R1_LOST_WAITING_REGAIN",
    "R50_HIT_TRACKING_R100", "R100_HIT_COMPLETE",
    "PERSISTABLE_ACTIVE_STATES", "POST_R1_ACTIVE_STATES", "TERMINAL_STATES",
    "EXPIRED_SESSION_CLOSE", "STOPPED_OUT", "is_terminal", "is_post_r1", "apply_transition",
    "apply_terminal_state", "presentation",
]
