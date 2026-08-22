# -*- coding: utf-8 -*-
"""V86AM shared radar contract and official-eligibility policy.

This module is deliberately dependency-light.  The live radar, historical
replay, lifecycle ledger and integrity tests all call the same functions.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping, Optional

try:
    from coverage_policy import coverage_is_official_blocking as _coverage_is_official_blocking
except Exception:  # pragma: no cover
    def _coverage_is_official_blocking(row):
        return any(_bool((row or {}).get(k)) for k in (
            "snapshot_official_blocked_by_coverage",
            "snapshot_coverage_incomplete",
            "snapshot_coverage_unknown",
            "snapshot_official_blocked_by_coverage_unknown",
        ))

OFFICIAL_POLICY_VERSION = "V86CL_R163_GANN20_EPISODE_EXECUTION_POLICY"
DEFAULT_SEALED_BREAKOUT_MIN_CONFIDENCE = 70.0
DEFAULT_EARLY_ARMED_MIN_CONFIDENCE = 78.0
DEFAULT_EARLY_ARMED_MIN_EPQ = 72.0

_BREAKOUT_POLICY_ALIASES = {
    "breakout": "breakout_only",
    "breakout_only": "breakout_only",
    "breakout-confirmation-only": "breakout_only",
    "sealed_gann_breakout_only": "breakout_only",
    "quality_elite_breakout": "quality_elite_breakout",
    "v86cd_quality_elite_breakout": "quality_elite_breakout",
    "three_signal": "quality_elite_breakout",
    "quality_elite_tracked_breakout": "quality_elite_breakout",
    "dual": "dual_tier_early_armed",
    "dual_tier": "dual_tier_early_armed",
    "early_armed": "dual_tier_early_armed",
    "sealed_gann_dual_tier": "dual_tier_early_armed",
    "sealed_gann_early_armed": "dual_tier_early_armed",
    "dual_tier_early_armed": "dual_tier_early_armed",
    # Retained only as a compatibility input.  Legacy remains strict breakout.
    "legacy": "breakout_only",
    "legacy_multi_type": "breakout_only",
    "execution_profit_layer": "execution_profit_layer",
    "r161_execution_profit_layer": "execution_profit_layer",
    "r162_execution_profit_layer": "execution_profit_layer",
}


def normalize_official_policy(value: Any) -> str:
    raw = str(value or "breakout_only").strip().lower()
    return _BREAKOUT_POLICY_ALIASES.get(raw, "breakout_only")


def _finite(value: Any, default: float = float("nan")) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _bool(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return bool(value)
    if isinstance(value, (int, float)):
        try:
            if not math.isfinite(float(value)):
                return False
            return float(value) != 0.0
        except Exception:
            return False
    if isinstance(value, str):
        raw = value.strip().lower()
        if raw in {"1", "true", "yes", "on", "y", "pass"}:
            return True
        if raw in {"", "0", "false", "no", "off", "n", "fail", "none", "nan", "null"}:
            return False
        return False
    return False


def evaluate_official_eligibility(
    row: Mapping[str, Any],
    *,
    official_policy: Any = "breakout_only",
    min_confidence: Any = DEFAULT_SEALED_BREAKOUT_MIN_CONFIDENCE,
    data_stale: Optional[bool] = None,
) -> Dict[str, Any]:
    """Return the one authoritative radar-official decision.

    ``official_rule_passed`` is the technical/historical truth and is
    independent of data freshness.  ``live_publishable`` adds the operational
    freshness constraint.  Therefore ``historical_would_be_official`` exactly
    matches the real live technical rule without allowing stale rows into the
    live lifecycle ledger.
    """
    policy = normalize_official_policy(official_policy or row.get("official_policy"))
    scope = str(row.get("radar_scope") or "").strip().upper()
    stage = str(row.get("radar_stage") or row.get("radar_signal_type") or "").strip().upper()
    confidence = _finite(row.get("confidence"), 0.0)
    min_conf = max(0.0, min(100.0, _finite(min_confidence, DEFAULT_SEALED_BREAKOUT_MIN_CONFIDENCE)))
    scope_sealed = scope.startswith("SEALED")
    bar_sealed = _bool(row.get("signal_bar_is_sealed"))
    # Fail closed: both the route scope and bar flag must prove sealing.
    sealed = bool(scope_sealed and bar_sealed)
    context_veto = _bool(row.get("gann_context_veto")) or _bool(row.get("context_veto"))
    snapshot_blocked = bool(_bool(row.get("snapshot_official_blocked")) or _coverage_is_official_blocking(row))
    policy_blocked = _bool(row.get("policy_blocked_official")) or snapshot_blocked
    grid_value = row.get("gann_grid_valid")
    # V86AM official rows require an explicit valid normalized-Gann grid.
    grid_ok = _bool(grid_value) if grid_value is not None else False
    live_review_blocked = any(
        _bool(row.get(key))
        for key in (
            "data_delayed",
            "snapshot_review_only",
            "saved_snapshot_review",
            "historical_review_forced_by_guard",
            "live_official_blocked_by_review",
            "snapshot_live_publishable_blocked",
        )
    )
    stale = (_bool(row.get("data_stale")) if data_stale is None else bool(data_stale)) or live_review_blocked

    execution_layer_ok = bool(
        policy == "execution_profit_layer"
        and _bool(row.get("execution_authorized"))
        and str(row.get("execution_authority") or "").upper() in {"VALIDATED_OFFICIAL", "VALIDATED_PAPER"}
        and (_bool(row.get("execution_passed")) or _bool(row.get("execution_publishable")) or "EXECUTION_AUTHORIZED" in str(row.get("professional_signal_type") or "").upper())
        and not policy_blocked
        and not context_veto
    )
    policy_ok = policy in {"breakout_only", "dual_tier_early_armed", "quality_elite_breakout", "execution_profit_layer"}
    breakout_ok = stage == "BREAKOUT_CONFIRMATION"
    signal_type = str(row.get("professional_signal_type") or row.get("radar_signal_type") or "").strip().upper()
    label = str(row.get("professional_signal_type_ar") or row.get("radar_tier_ar") or "").strip()
    gate = str(row.get("official_quality_gate") or "").strip().lower()
    quality_elite_ok = bool(
        policy == "quality_elite_breakout"
        and signal_type == "FAST_PULSE_MOMENT"
        and label in {"جودة", "نخبة"}
        and gate in {
            "quality_same_bar_watch_only",
            "elite_same_bar_watch_only",
            "quality_elite_watch_until_breakout",
            "quality_elite_official",
            "strong_unexhausted_quality",
        }
    )
    tracked_breakout_ok = bool(
        policy == "quality_elite_breakout"
        and signal_type == "TRACKED_BREAKOUT_CONFIRMATION"
        and "BREAKOUT" in stage
    )
    confidence_ok = confidence >= min_conf
    tracked_breakout_confidence_ok = bool(tracked_breakout_ok and confidence >= min(60.0, min_conf))
    early_min_conf = max(min_conf + 6.0, DEFAULT_EARLY_ARMED_MIN_CONFIDENCE)
    early_epq = _finite(row.get("early_pulse_quality_score"), 0.0)
    early_context_points = _finite(row.get("context_points"), 0.0)
    early_signal = str(row.get("radar_signal_type") or "").strip().upper()
    early_gate = str(row.get("official_quality_gate") or "").strip().lower()
    early_allowed_policy = policy == "dual_tier_early_armed"
    early_stage_ok = stage == "SEALED_ARMED"
    early_signal_ok = early_signal in {
        "POST_PULSE_FOLLOW_THROUGH",
        "EXTENDING_NEW_PULSE",
        "BREAKOUT_CONFIRMATION",
    }
    early_quality_ok = bool(
        confidence >= early_min_conf
        and early_epq >= DEFAULT_EARLY_ARMED_MIN_EPQ
        and early_context_points >= 5.0
        and "no_chase" not in early_gate
    )
    early_rule_passed = bool(
        early_allowed_policy
        and sealed
        and early_stage_ok
        and early_signal_ok
        and early_quality_ok
        and not context_veto
        and not policy_blocked
        and grid_ok
    )
    stage_ok = bool(breakout_ok or early_rule_passed or quality_elite_ok or tracked_breakout_ok or execution_layer_ok)
    rule_passed = bool(
        execution_layer_ok
        or (
            policy_ok
            and sealed
            and stage_ok
            and (confidence_ok or tracked_breakout_confidence_ok)
            and not context_veto
            and not policy_blocked
            and grid_ok
        )
    )
    live_publishable = bool(rule_passed and not stale)

    failures = []
    if not policy_ok:
        failures.append("UNSUPPORTED_OFFICIAL_POLICY")
    if not sealed and not execution_layer_ok:
        failures.append("NOT_SEALED")
    if not stage_ok:
        failures.append("NOT_BREAKOUT_OR_EARLY_ARMED")
    if not (confidence_ok or tracked_breakout_confidence_ok):
        failures.append("CONFIDENCE_BELOW_SEALED_MIN")
    if early_allowed_policy and early_stage_ok and not early_quality_ok:
        failures.append("EARLY_ARMED_QUALITY_GUARD")
    if context_veto:
        failures.append("CONTEXT_VETO")
    if snapshot_blocked:
        failures.append("SNAPSHOT_COVERAGE_BLOCKED")
    if policy_blocked and not snapshot_blocked:
        failures.append("POLICY_BLOCKED")
    if not grid_ok and not execution_layer_ok:
        failures.append("INVALID_GANN_GRID")
    if rule_passed and stale:
        failures.append("STALE_NOT_LIVE_PUBLISHABLE")

    if execution_layer_ok:
        official_tier = str(row.get("execution_tier") or row.get("execution_profile") or "EXECUTION_AUTHORIZED")
    elif early_rule_passed and not breakout_ok:
        official_tier = "EARLY_ARMED"
    elif tracked_breakout_ok and rule_passed:
        official_tier = "TRACKED_BREAKOUT"
    elif quality_elite_ok and rule_passed:
        official_tier = "QUALITY_ELITE"
    elif breakout_ok and rule_passed:
        official_tier = "CONFIRMED_BREAKOUT"
    else:
        official_tier = "NONE"

    return {
        "official_policy_version": OFFICIAL_POLICY_VERSION,
        "official_policy_normalized": policy,
        "official_rule_passed": rule_passed,
        "technical_rule_passed": rule_passed,
        "historical_would_be_official": rule_passed,
        "live_publishable": live_publishable,
        "official_min_confidence": float(min_conf),
        "official_tier": official_tier,
        "official_early_armed_enabled": bool(policy == "dual_tier_early_armed"),
        "official_early_armed_rule_passed": bool(early_rule_passed),
        "official_early_armed_min_confidence": float(early_min_conf),
        "official_early_armed_min_epq": float(DEFAULT_EARLY_ARMED_MIN_EPQ),
        "official_early_armed_epq_passed": bool(early_epq >= DEFAULT_EARLY_ARMED_MIN_EPQ),
        "official_early_armed_context_passed": bool(early_context_points >= 5.0),
        "official_scope_sealed": scope_sealed,
        "official_bar_sealed": bar_sealed,
        "official_stage_breakout": breakout_ok,
        "official_stage_early_armed": bool(early_stage_ok),
        "official_stage_quality_elite": bool(quality_elite_ok),
        "official_stage_tracked_breakout": bool(tracked_breakout_ok),
        "official_confidence_passed": confidence_ok,
        "official_tracked_breakout_confidence_passed": tracked_breakout_confidence_ok,
        "official_context_veto": context_veto,
        "official_policy_blocked": policy_blocked,
        "official_snapshot_blocked": snapshot_blocked,
        "official_grid_valid": grid_ok,
        "official_eligibility_failures": failures,
        "official_eligibility_source": "radar_contract_policy.evaluate_official_eligibility",
    }


def apply_official_eligibility(
    row: Mapping[str, Any],
    *,
    official_policy: Any = "breakout_only",
    min_confidence: Any = DEFAULT_SEALED_BREAKOUT_MIN_CONFIDENCE,
    data_stale: Optional[bool] = None,
) -> Dict[str, Any]:
    out = dict(row or {})
    snapshot_blocked = bool(_bool(out.get("snapshot_official_blocked")) or _coverage_is_official_blocking(out))
    if snapshot_blocked:
        out["snapshot_official_blocked"] = True
        out["policy_blocked_official"] = True
    result = evaluate_official_eligibility(
        out,
        official_policy=official_policy,
        min_confidence=min_confidence,
        data_stale=data_stale,
    )
    out.update(result)
    out["_ain_official"] = bool(result["live_publishable"])
    out["radar_official_evaluation"] = bool(result["live_publishable"])
    out["tradable"] = bool(result["live_publishable"])
    out["historical_would_be_official"] = bool(result["official_rule_passed"])
    return out


def evaluate_execution_bar(
    *,
    bar_high: Any,
    bar_low: Any,
    bar_close: Any = None,
    target_price: Any,
    stop_price: Any,
    tie_policy: str = "favor_sl",
) -> Dict[str, Any]:
    """Evaluate one OHLC bar against an already frozen execution grid."""
    hi = _finite(bar_high)
    lo = _finite(bar_low)
    close = _finite(bar_close)
    target = _finite(target_price)
    stop = _finite(stop_price)
    tie = str(tie_policy or "favor_sl").strip().lower()

    target_touched = bool(
        math.isfinite(target)
        and ((math.isfinite(hi) and hi >= target) or (not math.isfinite(hi) and math.isfinite(close) and close >= target))
    )
    stop_touched = bool(
        math.isfinite(stop)
        and ((math.isfinite(lo) and lo <= stop) or (not math.isfinite(lo) and math.isfinite(close) and close <= stop))
    )

    if target_touched and stop_touched:
        if tie == "favor_tp":
            outcome, exit_price = "TP_HIT", target
        else:
            outcome, exit_price = "SL_HIT_TIE_FAVOR_SL", stop
    elif stop_touched:
        outcome, exit_price = "SL_HIT", stop
    elif target_touched:
        outcome, exit_price = "TP_HIT", target
    else:
        outcome, exit_price = "NO_TOUCH", float("nan")

    return {
        "outcome": outcome,
        "target_touched": target_touched,
        "stop_touched": stop_touched,
        "exit_price": exit_price,
        "tie_policy": tie,
    }
