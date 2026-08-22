# -*- coding: utf-8 -*-
"""V86CL R166 — exact-observation live sniper pulse and sealed R1 transition.

The engine is Qt-free and intentionally small.  It receives one-symbol tail
observations from the PriceTape path, exposes a live pulse only when BOTH:

1) RSIScaled/VAR3 positive cross is active on the forming bar, and
2) the provisional P(R50/20) reaches the configured market threshold.

The first qualifying time/price are immutable.  When the next bar appears, the
same pulse id is either confirmed from the now-sealed bar or rejected.  The
provisional pulse is official-for-display but explicitly non-executable; R1 and
portfolio execution policy are not changed by this module.
"""
from __future__ import annotations
from exception_observability import report_suppressed_exception as _report_suppressed_exception
from strict_jsonl import append_jsonl as _append_jsonl_strict

import copy
import datetime as _dt
import json
import math
import os
import re
import threading
import time
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from runtime_session import record_stage_error as _record_stage_error
from column_truth_contract import (
    verified_sealed_close as _truth_verified_sealed_close,
    stamp_exact_sealed_bar as _truth_stamp_exact_sealed_bar,
    wall_time_key as _truth_wall_time_key,
    apply_trader_column_truth as _truth_apply_trader_columns,
    effective_financial_truth as _truth_effective_financial,
)
from probability_performance_contract import (
    feature_vector_sha256 as _probability_feature_vector_sha256,
    prepare_probability_market_snapshot as _prepare_probability_market_snapshot,
)
from probability_causal_audit import (
    STAGE_BIRTH as _A95_STAGE_BIRTH,
    build_score_evidence as _a95_build_score_evidence,
    score_stage as _a95_score_stage,
)
from live_episode_truth_contract import (
    TRUTH_LIFECYCLE, TRUTH_LIVE_SOURCE, TRUTH_SEALED, TRUTH_R1_ACTIVATION, TRUTH_TERMINAL,
    stamp_truth as _stamp_episode_truth,
)
from pre_entry_stop_contract import PRE_ENTRY_STOP_INVALIDATED
from live_signal_state_taxonomy import is_terminal as _taxonomy_is_terminal, active_counts as _taxonomy_active_counts
from market_datetime_normalizer import to_market_naive as _market_to_naive
from terminal_truth_authority import (
    load_terminal_truths as _load_terminal_truths,
    terminal_truth_for_episode as _terminal_truth_for_episode,
)

from sealed_close_lineage_contract import sealed_close_audit_fields, sealed_close_lineage_row_fields
_PROBABILITY_STAGE_TIMING = threading.local()


def _store_probability_stage_timing(timing: Mapping[str, Any]) -> None:
    _PROBABILITY_STAGE_TIMING.value = dict(timing or {})


def consume_probability_stage_timing() -> Dict[str, Any]:
    value = dict(getattr(_PROBABILITY_STAGE_TIMING, "value", {}) or {})
    _PROBABILITY_STAGE_TIMING.value = {}
    return value


try:
    from r159_pipeline_core import canonical_pulse_id, classify_probability_kind, level_reached, level_reached, touch_level_state
except Exception:
    canonical_pulse_id = None
    level_reached = None
    touch_level_state = None
    def classify_probability_kind(target_symbols, total_symbols, **kwargs): return "PARTIAL_MARKET_PROVISIONAL"

try:
    from execution_profit_layer import (
        VERSION as EXECUTION_LAYER_VERSION,
        evaluate_execution_layer as _evaluate_execution_layer,
        flatten_execution_result as _flatten_execution_result,
    )
except Exception:
    EXECUTION_LAYER_VERSION = "UNAVAILABLE"
    _evaluate_execution_layer = None
    _flatten_execution_result = None

try:
    from gann20_episode_contract import (
        VERSION as GANN20_CONTRACT_VERSION, HORIZON_BARS, DISCARD_BELOW_PCT,
        PROB_SCOPE_LIVE_CURRENT_BAR, PROB_SCOPE_SEALED_CROSS_BAR,
        display_threshold_pct as _contract_threshold_pct,
        should_display_live as _should_display_live,
        should_arm_r1_after_seal as _should_arm_r1_after_seal,
        infer_probability_scope as _infer_probability_scope,
        exact_probability_for_observation as _exact_probability_for_observation,
        contract_fields as _contract_fields,
        stamp_live_probability as _stamp_live_probability,
        stamp_sealed_probability as _stamp_sealed_probability,
        canonical_episode_id as _canonical_episode_id,
    )
    from gann20_episode_ledger import append_event as _append_episode_event
    from m30_sniper_birth_seal_contract import should_arm_live_r1_at_birth as _should_arm_r1_at_birth
except Exception:  # pragma: no cover
    GANN20_CONTRACT_VERSION = "unavailable"
    HORIZON_BARS = 20
    DISCARD_BELOW_PCT = 20.0
    PROB_SCOPE_LIVE_CURRENT_BAR = "LIVE_CURRENT_BAR"
    PROB_SCOPE_SEALED_CROSS_BAR = "SEALED_CROSS_BAR"
    def _contract_threshold_pct(market_key): return 30.0
    def _should_display_live(p50, market_key): return math.isfinite(_finite(p50)) and _finite(p50) >= _contract_threshold_pct(market_key)
    def _should_arm_r1_at_birth(p50, market_key): return math.isfinite(_finite(p50)) and 20 <= _finite(p50) < _contract_threshold_pct(market_key)
    def _should_arm_r1_after_seal(p50, market_key): return math.isfinite(_finite(p50)) and 20 <= _finite(p50) < _contract_threshold_pct(market_key)
    def _infer_probability_scope(probability, observation=None, sealed=False): return PROB_SCOPE_SEALED_CROSS_BAR if sealed else "UNAVAILABLE"
    def _exact_probability_for_observation(probability, observation): return False
    def _contract_fields(**kwargs): return {"model_horizon_bars": 20}
    def _stamp_live_probability(probability, observation, calculated_at=None): return dict(probability or {})
    def _stamp_sealed_probability(probability, bar_time=None, calculated_at=None, source_observation_id=None): return dict(probability or {})
    def _canonical_episode_id(market_key, symbol, bar): return f"GANN20-{market_key}-{symbol}-{bar}"
    def _append_episode_event(*args, **kwargs): return {"written": False}

from live_sniper_contract import (
    VERSION as LIVE_SNIPER_CONTRACT_VERSION,
    LIVE_P30_BORN, LIVE_P30_UPDATED, SEALED_P30_CONFIRMED as SNIPER_SEALED_P30_CONFIRMED,
    SEALED_P30_LATE_CENTER_ONLY as SNIPER_SEALED_P30_LATE_CENTER_ONLY,
    P30_DOWNGRADED_TO_R1_WATCH, SEALED_R1_WATCH_ARMED,
    ORIGIN_LIVE_SOURCE_OBSERVATION, ORIGIN_SEALED_CROSS_BAR,
)
from durable_r1_lifecycle import (
    R1_ACTIVE_WAITING_R50, R1_LOST_WAITING_REGAIN,
    R50_HIT_TRACKING_R100, R100_HIT_COMPLETE,
    EPISODE_EXPIRED_20_BARS, EPISODE_CLOSED_NEGATIVE_CROSS, STOPPED_OUT,
    POST_R1_ACTIVE_STATES,
)

VERSION = "A4_2_14_TRUTH_BOUNDARY_HOTFIX3_SEAL_ENGINE_V1"
CAUSAL_AUDIT_VERSION = "A95_BIRTH_SEAL_CAUSAL_AUDIT_CONTRACT_V1"
MAX_PENDING_SEALS_PER_SYMBOL = 8

LIVE_INTERNAL_PENDING_SEAL = "LIVE_INTERNAL_PENDING_SEAL"
LIVE_WAITING_R1 = "LIVE_WAITING_R1"
LIVE_PENDING_SEAL = "LIVE_PENDING_SEAL"
SEAL_AWAITING_FINAL_SCORE = "SEAL_AWAITING_FINAL_SCORE"
INTRABAR_CROSS_FAILED_AT_SEAL = "INTRABAR_CROSS_FAILED_AT_SEAL"
SEALED_P30_CONFIRMED = "SEALED_P30_CONFIRMED"
SEALED_WAITING_R1 = "SEALED_WAITING_R1"
SEALED_P30_LATE_CENTER_ONLY = "SEALED_P30_LATE_CENTER_ONLY"
SEALED_REJECTED = "SEALED_REJECTED"
TARGET_CONSUMED_BEFORE_ENTRY = "TARGET_CONSUMED_BEFORE_ENTRY"
TARGET_CONSUMED_BEFORE_OFFICIAL = "TARGET_CONSUMED_BEFORE_OFFICIAL"
LIVE_LATE_NO_CHASE = "LIVE_LATE_NO_CHASE"
LIVE_FAILED = "LIVE_FAILED"
LIVE_WEAKENING = "LIVE_WEAKENING"
EXPIRED_SESSION_CLOSE = "EXPIRED_SESSION_CLOSE"

FORMING_STATES = {LIVE_INTERNAL_PENDING_SEAL, LIVE_WAITING_R1, LIVE_PENDING_SEAL, LIVE_WEAKENING}
ACTIVE_SEALED_STATES = {SEALED_P30_CONFIRMED, SEALED_WAITING_R1}
ACTIVE_POST_R1_STATES = set(POST_R1_ACTIVE_STATES)
PERSISTED_ACTIVE_STATES = {SEAL_AWAITING_FINAL_SCORE, *FORMING_STATES, *ACTIVE_SEALED_STATES, *ACTIVE_POST_R1_STATES}
FINAL_SCORE_RETRY_STATES = {SEAL_AWAITING_FINAL_SCORE, *FORMING_STATES}
TERMINAL_STATES = {
    INTRABAR_CROSS_FAILED_AT_SEAL,
    SEALED_P30_LATE_CENTER_ONLY, SEALED_REJECTED, R100_HIT_COMPLETE,
    TARGET_CONSUMED_BEFORE_ENTRY, TARGET_CONSUMED_BEFORE_OFFICIAL,
    LIVE_LATE_NO_CHASE, LIVE_FAILED, EXPIRED_SESSION_CLOSE, PRE_ENTRY_STOP_INVALIDATED,
    EPISODE_EXPIRED_20_BARS, EPISODE_CLOSED_NEGATIVE_CROSS, STOPPED_OUT,
}


class SealPayloadConflict(RuntimeError):
    """Same Episode/bar was presented with a different sealed truth."""



def _finite(value: Any, default: float = float("nan")) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _truthy(value: Any) -> bool:
    """Return a stable boolean for pandas/numpy/scalar values."""
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    try:
        if value != value:  # NaN / pandas NA where comparison is defined
            return False
    except Exception as _suppressed_exc:
        _report_suppressed_exception(
            _suppressed_exc, module=__name__, file=__file__,
            function='_truthy', line=155,
            stage='model_score', critical=True,
        )
    try:
        return bool(value)
    except Exception:
        return False


def _missing_probability_from_scored_row(
    row: Mapping[str, Any], *, feature_count_received: int, sealed: bool
) -> Dict[str, Any]:
    """Classify a missing score without inventing an internal model error."""
    getter = row.get if hasattr(row, "get") else (lambda _key, _default=None: _default)
    technical_signal = _truthy(getter("technical_signal_live"))
    if not technical_signal:
        if sealed:
            return {
                "available": False,
                "probability_attempted": True,
                "error": "sealed_cross_not_stable",
                "failure_stage": "CROSS_DETECTION",
                "failure_reason_code": "CROSS_NOT_STABLE_AT_SEAL",
                "error_type": "SealedCrossNotStable",
                "error_message": "The intrabar RSIScaled/VAR cross was no longer present on the exact sealed signal bar.",
                "feature_count_received": int(feature_count_received),
            }
        return {
            "available": False,
            "probability_attempted": True,
            "error": "cross_not_active_at_score",
            "failure_stage": "CROSS_DETECTION",
            "failure_reason_code": "CROSS_NOT_ACTIVE_AT_SCORE",
            "error_type": "CrossNotActiveAtScore",
            "error_message": "The queued intrabar cross was no longer active when the model batch reached this observation.",
            "feature_count_received": int(feature_count_received),
        }
    model_error = str(getter("gann20_model_error") or "model_score_output_missing")
    return {
        "available": False,
        "probability_attempted": True,
        "error": model_error,
        "failure_stage": "MODEL_SCORE",
        "failure_reason_code": "MODEL_SCORE_OUTPUT_MISSING",
        "error_type": "ProbabilityOutputMissing",
        "error_message": "The technical signal was present but the model returned no finite probability.",
        "feature_count_received": int(feature_count_received),
    }


def _iso(value: Any = None) -> str:
    if value is None:
        value = _dt.datetime.now(_dt.timezone.utc)
    try:
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if isinstance(value, _dt.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=_dt.timezone.utc)
            return value.isoformat(timespec="milliseconds")
    except Exception as _suppressed_exc:
        _report_suppressed_exception(
            _suppressed_exc, module=__name__, file=__file__,
            function='_iso', line=214,
            stage='model_score', critical=True,
        )
    return str(value or "")


def _safe_slug(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9_\-]+", "_", str(value or "market")).strip("_")
    return text[:96] or "market"


def _market_family(market_key: str) -> str:
    try:
        from market_key_contract import gann_market_family
        return gann_market_family(market_key)
    except Exception:
        return "generic"


def threshold_pct_for_market(market_key: str) -> float:
    """Compatibility wrapper around the single R163 contract source."""
    return float(_contract_threshold_pct(market_key))


def seal_stable_observations_required() -> int:
    """Required identical *stable file reads* for an exact M30 seal.

    Local updates are irregular, so one snapshot is trusted only after stat-before/
    stat-after proves the file unchanged; a lab override may require more reads.
    """
    try:
        return max(1, min(5, int(os.environ.get("AIN_R169368_SEAL_STABLE_READS", "1") or "1")))
    except Exception:
        return 1


def _seal_fingerprint(final_obs: Mapping[str, Any], final_prob: Mapping[str, Any]) -> Tuple[Any, ...]:
    return (
        round(_finite(final_obs.get("open")), 4),
        round(_finite(final_obs.get("high")), 4),
        round(_finite(final_obs.get("low")), 4),
        round(_finite(final_obs.get("close")), 4),
        round(_finite(final_obs.get("volume"), 0.0), 2),
        round(_finite(final_prob.get("p50_pct")), 4),
        round(_finite(final_prob.get("p100_pct")), 4),
        bool(final_obs.get("positive_cross_now")),
        round(_finite(final_obs.get("pulse_gap")), 6),
        round(_finite(final_obs.get("previous_gap")), 6),
    )


def _linked_paac_snapshot(row: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    """Keep only the PAAC truth tied to the LP at qualification/update."""
    src = dict(row or {})
    episode_id = str(src.get("pulse_episode_id") or src.get("id") or src.get("paac_episode_id") or "")
    # R2: state may arrive through PAAC, action_state, or the UI-facing stage.
    state = str(
        src.get("pulse_acceptance_state")
        or src.get("paac_state")
        or src.get("action_state")
        or src.get("live_pulse_seal_state")
        or src.get("state")
        or src.get("radar_stage")
        or ""
    )
    out: Dict[str, Any] = {
        "episode_id": episode_id,
        "signal_bar_time": str(src.get("signal_bar_time") or src.get("recommendation_datetime") or ""),
        "state": state,
        "state_changed_at": str(src.get("state_changed_at") or src.get("last_update") or ""),
        "accepted_at": str(src.get("accepted_at") or ""),
        "trend_confirmed_at": str(src.get("trend_confirmed_at") or ""),
        "late_no_chase_reason_code": str(src.get("late_no_chase_reason_code") or src.get("no_chase_reason_code") or src.get("pulse_acceptance_reason") or src.get("reason_code") or ""),
        "pulse_acceptance_reason": str(src.get("pulse_acceptance_reason") or src.get("reason") or ""),
    }
    for key in (
        "pulse_avwap", "retention_50", "defense_38", "anchor_low", "atr14",
        "distance_from_cross_atr", "bar_extension_atr", "move_since_cross_pct",
        "recent_runup_pct", "upper_wick", "rejection",
        "first_cross_at", "first_cross_price", "technical_quality",
    ):
        value = src.get(key)
        if value not in (None, ""):
            out[key] = value
    # Keep an otherwise anonymous no-chase/failed state; it is still truth.
    if not out.get("episode_id") and any(x in state.upper() for x in ("LIVE_LATE_NO_CHASE", "LIVE_FAILED", "LIVE_WEAKENING")):
        out["episode_id"] = f"PAAC-{hash(state) & 0xffffffff:x}"
    return out if any(v not in (None, "") for v in out.values()) else {}


def _paac_terminal_state(snapshot: Optional[Mapping[str, Any]]) -> str:
    snap = dict(snapshot or {})
    state = str(snap.get("state") or "").upper()
    reason = str(snap.get("pulse_acceptance_reason") or snap.get("late_no_chase_reason_code") or "").upper()
    joined = f"{state}|{reason}"
    if "LIVE_LATE_NO_CHASE" in joined or "NO_CHASE" in joined or "لا تطارد" in str(snap):
        return "LIVE_LATE_NO_CHASE"
    if "LIVE_FAILED" in joined or "FAILED" in joined:
        return "LIVE_FAILED"
    if "LIVE_WEAKENING" in joined or "WEAKEN" in joined or "ضعف" in str(snap):
        return "LIVE_WEAKENING"
    return ""


def _level_reached(price: Any, level: Any, *, high: Any = None, market_key: str = "", symbol: str = "") -> bool:
    try:
        if callable(level_reached):
            return bool(level_reached(price, level, high=high, market_key=market_key, symbol=symbol))
    except Exception as exc:
        _record_stage_error(
            "cross_detection", "evaluate_level_reached", exc,
            market=str(market_key), symbol=str(symbol),
            reason_code="LEVEL_REACHED_HELPER_FAILED",
        )
    lvl = _finite(level)
    if not math.isfinite(lvl) or lvl <= 0:
        return False
    val = _finite(high, _finite(price))
    return math.isfinite(val) and val >= lvl


def _target_consumed_probe(pulse_or_levels: Any, obs: Mapping[str, Any], *, market_key: str = "", symbol: str = "") -> Dict[str, Any]:
    levels = dict(getattr(pulse_or_levels, "levels", pulse_or_levels) or {})
    price = _finite(obs.get("close"), _finite(obs.get("current_price"), _finite(obs.get("price"))))
    high = _finite(obs.get("source_high"), _finite(obs.get("signal_bar_high"), _finite(obs.get("high"), price)))
    low = _finite(obs.get("source_low"), _finite(obs.get("signal_bar_low"), _finite(obs.get("low"), price)))
    r1 = _finite(levels.get("r1_price"), _finite(levels.get("gann_r1_breakout_point")))
    r50 = _finite(levels.get("r50_price"), _finite(levels.get("gann20_target50_price"), _finite(levels.get("gann_r3_resistance_50"))))
    r100 = _finite(levels.get("r100_price"), _finite(levels.get("gann_r5_resistance_100")))
    r1_hit = _level_reached(price, r1, high=high, market_key=market_key, symbol=symbol)
    r50_hit = _level_reached(price, r50, high=high, market_key=market_key, symbol=symbol)
    r100_hit = _level_reached(price, r100, high=high, market_key=market_key, symbol=symbol)
    return {
        "price": price, "high": high, "low": low, "r1_price": r1, "r50_price": r50, "r100_price": r100,
        "activated_r1": bool(r1_hit), "hit_r50": bool(r50_hit), "hit_r100": bool(r100_hit),
        "target_consumed": bool(r1_hit and r50_hit),
    }

def _target_bar_symbol_count(work: Any, target_bar_time: Any) -> int:
    """Count symbols actually present on the target bar, not in all history."""
    try:
        import pandas as pd
        if work is None or getattr(work, "empty", True) or "date" not in work.columns or "symbol" not in work.columns:
            return 0
        target = pd.to_datetime(target_bar_time, errors="coerce", utc=True)
        if pd.isna(target):
            return 0
        dates = pd.to_datetime(work["date"], errors="coerce", utc=True)
        mask = dates.eq(target)
        return int(work.loc[mask, "symbol"].astype(str).str.upper().nunique())
    except Exception:
        return 0


def _journal_root() -> Path:
    raw = str(os.environ.get("AIN_LIVE_PULSE_SEAL_DIR", "") or "").strip()
    if raw:
        return Path(raw)
    return Path(__file__).resolve().parent / "datainfo" / "live_pulse_seal"


def _append_jsonl(path: Path, row: Mapping[str, Any]) -> None:
    try:
        return bool(_append_jsonl_strict(Path(path), dict(row or {})))
    except Exception as exc:
        _record_stage_error("event_write", "live_pulse_seal_append", exc, market=str(row.get("market_key") or row.get("market") or ""), symbol=str(row.get("symbol") or ""), episode_id=str(row.get("pulse_episode_id") or row.get("episode_id") or ""), source_observation_id=str(row.get("source_observation_id") or ""), reason_code="LIVE_PULSE_SEAL_JSONL_APPEND_FAILED")
        return False
def _paac_state_text(link_or_row: Optional[Mapping[str, Any]]) -> str:
    src = dict(link_or_row or {})
    return str(
        src.get("state")
        or src.get("pulse_acceptance_state")
        or src.get("action_state")
        or src.get("radar_stage")
        or ""
    ).strip().upper()


def _is_late_no_chase(link_or_row: Optional[Mapping[str, Any]]) -> bool:
    return _paac_state_text(link_or_row) == LIVE_LATE_NO_CHASE


def _is_live_failed(link_or_row: Optional[Mapping[str, Any]]) -> bool:
    return _paac_state_text(link_or_row) == LIVE_FAILED


def _is_live_weakening(link_or_row: Optional[Mapping[str, Any]]) -> bool:
    return _paac_state_text(link_or_row) == LIVE_WEAKENING


def _best_value(mapping: Mapping[str, Any], keys: Iterable[str], default: float = float("nan")) -> float:
    for key in keys:
        val = _finite(mapping.get(key), float("nan"))
        if math.isfinite(val):
            return val
    return default


def _best_high(mapping: Mapping[str, Any], *, fallback_price: float = float("nan")) -> float:
    vals = []
    for key in ("probability_result_current_high", "source_high", "signal_bar_high", "high", "current_high", "bar_high", "latest_high"):
        val = _finite(mapping.get(key), float("nan"))
        if math.isfinite(val):
            vals.append(val)
    if math.isfinite(fallback_price):
        vals.append(fallback_price)
    return max(vals) if vals else float("nan")


def _best_low(mapping: Mapping[str, Any], *, fallback_price: float = float("nan")) -> float:
    vals = []
    for key in ("probability_result_current_low", "source_low", "signal_bar_low", "low", "current_low", "bar_low", "latest_low"):
        val = _finite(mapping.get(key), float("nan"))
        if math.isfinite(val):
            vals.append(val)
    if math.isfinite(fallback_price):
        vals.append(fallback_price)
    return min(vals) if vals else float("nan")


def _level_from(levels: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        val = _finite(levels.get(key), float("nan"))
        if math.isfinite(val) and val > 0:
            return val
    return float("nan")


def _touch(price: Any, level: Any, *, high: Any = None, market_key: str = "", symbol: str = "") -> bool:
    if callable(level_reached):
        try:
            return bool(level_reached(price, level, high=high, market_key=market_key, symbol=symbol))
        except Exception as exc:
            _record_stage_error(
                "cross_detection", "evaluate_touch_level", exc,
                market=str(market_key), symbol=str(symbol),
                reason_code="TOUCH_LEVEL_HELPER_FAILED",
            )
    lvl = _finite(level)
    if not math.isfinite(lvl) or lvl <= 0:
        return False
    price_v = _finite(price)
    high_v = _finite(high, price_v)
    return bool(math.isfinite(high_v) and high_v + 1e-9 >= lvl)



def _execution_eval_for_pulse(
    pulse: Optional["_Pulse"],
    observation: Optional[Mapping[str, Any]] = None,
    probability: Optional[Mapping[str, Any]] = None,
    *,
    paac_row: Optional[Mapping[str, Any]] = None,
    extra_context: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Evaluate the market-specific execution layer without breaking live monitoring.

    The radar remains useful even when execution context is incomplete, so any
    execution-layer failure is returned as RADAR_ONLY/ENGINE_UNAVAILABLE rather
    than raising through the price tape.
    """
    if pulse is None or not callable(_evaluate_execution_layer):
        return {
            "execution_layer_version": EXECUTION_LAYER_VERSION,
            "execution_decision": "RADAR_ONLY",
            "execution_passed": False,
            "execution_tier": "EXECUTION_LAYER_UNAVAILABLE",
            "execution_blockers": ["EXECUTION_LAYER_UNAVAILABLE"],
            "entry_status_ar": "رادار فقط — طبقة التنفيذ غير متاحة",
            "entry_status_code": "EXECUTION_LAYER_UNAVAILABLE",
        }
    obs: Dict[str, Any] = {}
    for src in (observation, extra_context, pulse.audit if pulse is not None else None):
        if isinstance(src, Mapping):
            obs.update({k: v for k, v in src.items() if v not in (None, "")})
    obs.setdefault("market_key", pulse.market_key)
    obs.setdefault("symbol", pulse.symbol)
    obs.setdefault("name", pulse.name)
    obs.setdefault("current_price", pulse.current_price)
    obs.setdefault("close", pulse.current_price)
    obs.setdefault("appearance_price", pulse.appearance_price)
    obs.setdefault("p50_live", pulse.p50_live)
    obs.setdefault("p100_live", pulse.p100_live)
    obs.setdefault("p50_at_cross", pulse.p50_at_qualification)
    levels = dict(pulse.levels or {})
    prob: Dict[str, Any] = {}
    if isinstance(probability, Mapping):
        prob.update({k: v for k, v in probability.items() if v not in (None, "")})
    if "p50_pct" not in prob:
        p50 = pulse.p50_final if math.isfinite(pulse.p50_final) else pulse.p50_live
        prob["p50_pct"] = p50
    if "p100_pct" not in prob:
        p100 = pulse.p100_final if math.isfinite(pulse.p100_final) else pulse.p100_live
        prob["p100_pct"] = p100
    # Expose terminal state as a hard blocker to the execution layer.
    obs["live_pulse_seal_state"] = pulse.state
    try:
        return dict(_evaluate_execution_layer(
            pulse.market_key,
            obs,
            prob,
            levels=levels,
            paac_row=paac_row if paac_row is not None else pulse.linked_paac_snapshot,
        ) or {})
    except Exception as exc:
        return {
            "execution_layer_version": EXECUTION_LAYER_VERSION,
            "execution_decision": "RADAR_ONLY",
            "execution_passed": False,
            "execution_tier": "EXECUTION_LAYER_ERROR",
            "execution_blockers": ["EXECUTION_LAYER_ERROR"],
            "entry_status_ar": "رادار فقط — تعذر تقييم طبقة التنفيذ",
            "entry_status_code": "EXECUTION_LAYER_ERROR",
            "execution_error": str(exc)[:240],
        }

def _birth_and_current_price(obs: Mapping[str, Any]) -> Tuple[float, float]:
    birth = _finite(obs.get("close"), _finite(obs.get("current_price")))
    if obs.get("probability_result_price_available") is False:
        # H12H14H5 defense-in-depth: an explicitly unavailable late-price
        # authority can never be substituted with the historical Birth close.
        return birth, float("nan")
    return birth, _finite(obs.get("probability_result_current_price"), birth)


def _execution_price_truth(obs: Mapping[str, Any]) -> Tuple[float, float, float]:
    _birth, price = _birth_and_current_price(obs)
    if bool(obs.get("probability_result_guard_price_only")):
        return price, price, price
    return price, _best_high(obs, fallback_price=price), _best_low(obs, fallback_price=price)


def _level_truth(levels: Mapping[str, Any], obs: Mapping[str, Any], *, market_key: str, symbol: str) -> Dict[str, Any]:
    price, high, low = _execution_price_truth(obs)
    r1 = _level_from(levels, "r1_price", "gann_r1_breakout_point", "r1_frozen")
    r50 = _level_from(levels, "r50_price", "gann20_target50_price", "gann_r3_resistance_50", "r50_frozen")
    r100 = _level_from(levels, "r100_price", "gann20_target100_price", "gann_r5_resistance_100", "r100_frozen")
    return {
        "price": price,
        "high": high,
        "low": low,
        "r1_price": r1,
        "r50_price": r50,
        "r100_price": r100,
        "activated_r1": _touch(price, r1, high=high, market_key=market_key, symbol=symbol),
        "hit_r50": _touch(price, r50, high=high, market_key=market_key, symbol=symbol),
        "hit_r100": _touch(price, r100, high=high, market_key=market_key, symbol=symbol),
    }


def _parse_bar_time(value: Any, market_key: str = "") -> Optional[_dt.datetime]:
    try:
        return _market_to_naive(value, market_key=market_key or "local_السوق السعودي")
    except Exception:
        return None


_BAR_WINDOW_FIELDS = (
    "bar_label_mode", "forming_window_start", "forming_window_end",
    "canonical_market_bar_time", "expected_current_bar_label",
    "bar_label_lag_bars", "source_bar_context_reason_code",
    "source_latency_class", "source_live_eligible",
    "exchange_window_is_forming", "source_window_in_market_session",
)


def _bar_window_contract(source: Mapping[str, Any]) -> Dict[str, Any]:
    return {key: (source or {}).get(key) for key in _BAR_WINDOW_FIELDS if (source or {}).get(key) not in (None, "")}


def _backfill_bar_window(target: Dict[str, Any], source: Mapping[str, Any]) -> None:
    for key, value in _bar_window_contract(source).items():
        target.setdefault(key, value)


@dataclass
class _Pulse:
    pulse_id: str
    market_key: str
    symbol: str
    name: str
    bar_time: str
    threshold_pct: float
    appearance_at: str
    appearance_price: float
    first_cross_at: str
    first_cross_price: float
    p50_at_first_cross: float
    p100_at_first_cross: float
    qualified_at: str
    qualification_price: float
    p50_at_qualification: float
    p100_at_qualification: float
    p50_live: float
    p100_live: float
    current_price: float
    p50_live_latest: float = float("nan")
    p100_live_latest: float = float("nan")
    source_mtime_ns: int = 0
    state: str = LIVE_PENDING_SEAL
    p50_final: float = float("nan")
    p100_final: float = float("nan")
    probability_scope_first_cross: str = ""
    probability_scope_live: str = ""
    probability_scope_sealed: str = ""
    probability_asof_first_cross: str = ""
    probability_asof_live: str = ""
    probability_asof_sealed: str = ""
    sealed_model_anchor_at: str = ""
    sealed_model_anchor_price: float = float("nan")
    model_bars_elapsed: int = 0
    signal_bar_close: float = float("nan")
    signal_bar_high: float = float("nan")
    sealed_at: str = ""
    last_seen_at: str = ""
    last_emit_signature: Tuple[Any, ...] = field(default_factory=tuple)
    levels: Dict[str, Any] = field(default_factory=dict)
    live_levels: Dict[str, Any] = field(default_factory=dict)
    sealed_levels: Dict[str, Any] = field(default_factory=dict)
    live_sniper_birth_proven: bool = False
    live_sniper_born_at: str = ""
    live_sniper_source_observation_id: str = ""
    audit: Dict[str, Any] = field(default_factory=dict)
    linked_paac_episode_id: str = ""
    linked_paac_snapshot: Dict[str, Any] = field(default_factory=dict)



def _clone_pulse(pulse: _Pulse) -> _Pulse:
    return copy.deepcopy(pulse)


def _replace_pulse_from_proposal(target: _Pulse, proposal: _Pulse) -> None:
    # Preserve object identity used by _by_symbol/_by_id while atomically
    # publishing a proposal that has already crossed the durable authority gate.
    for item in fields(_Pulse):
        setattr(target, item.name, copy.deepcopy(getattr(proposal, item.name)))


def _commit_terminal_proposal(
    engine: Any, pulse: _Pulse, proposal: _Pulse, event_type: str,
    extra: Optional[Dict[str, Any]] = None,
) -> None:
    if proposal.state not in TERMINAL_STATES:
        raise ValueError(f"NON_TERMINAL_PROPOSAL:{proposal.state}")
    proposal.audit = dict(proposal.audit or {})
    proposal.audit.setdefault("terminal_transition_prior_state", str(pulse.state or ""))
    # The Terminal SQLite/WAL commit (and its projection debt) occurs inside
    # _journal before the live object is changed.  A commit failure therefore
    # leaves the prior object retryable.
    engine._journal(proposal, event_type, dict(extra or {}))
    _replace_pulse_from_proposal(pulse, proposal)


def _a101_post_r1_presentation(pulse: _Pulse, state: str) -> Optional[Tuple[Any, ...]]:
    p50 = pulse.p50_final if math.isfinite(pulse.p50_final) else pulse.p50_live
    p100 = pulse.p100_final if math.isfinite(pulse.p100_final) else pulse.p100_live
    values = {
        R1_ACTIVE_WAITING_R50: ("اختراق R1 حي — ينتظر R50", "تحقق R1 — المتابعة نشطة", "R1_ACTIVE_WAITING_R50", "GANN20_R1_ACTIVE", "تحقق R1", "R1_ACTIVATION", "R1_ACTIVE_WAITING_R50"),
        R1_LOST_WAITING_REGAIN: ("فقد R1 — ينتظر الاستعادة", "فقد R1 بعد التفعيل — المتابعة مستمرة", "R1_LOST_WAITING_REGAIN", "GANN20_R1_LOST", "فقد R1", "R1_LIFECYCLE", "R1_LOST_WAITING_REGAIN"),
        R50_HIT_TRACKING_R100: ("بلغ R50 — يتابع R100", "تحقق R50 — الحلقة ما زالت تحت المتابعة", "R50_HIT_TRACKING_R100", "GANN20_R50_TRACKING_R100", "بلغ R50", "R1_LIFECYCLE", "R50_HIT_TRACKING_R100"),
        R100_HIT_COMPLETE: ("اكتملت الحلقة — تحقق R100", "تحقق R100 — انتهت المتابعة", "R100_HIT_COMPLETE", "GANN20_R100_COMPLETE", "تحقق R100", "TERMINAL_FINALIZER", "R100_HIT_COMPLETE"),
        EPISODE_EXPIRED_20_BARS: ("انتهى أفق الحلقة — 20 شمعة", "انتهت المتابعة بانقضاء الأفق", "EPISODE_EXPIRED_20_BARS", "GANN20_EPISODE_EXPIRED", "انتهى الأفق", "TERMINAL_FINALIZER", "EPISODE_EXPIRED_20_BARS"),
        EPISODE_CLOSED_NEGATIVE_CROSS: ("انتهت الحلقة — تقاطع سلبي", "أغلقت المتابعة بتقاطع سلبي", "EPISODE_CLOSED_NEGATIVE_CROSS", "GANN20_NEGATIVE_CROSS_CLOSED", "تقاطع سلبي", "TERMINAL_FINALIZER", "EPISODE_CLOSED_NEGATIVE_CROSS"),
        PRE_ENTRY_STOP_INVALIDATED: ("انتهت — ضُرب الوقف قبل الدخول", "أُبطلت الفرصة قبل الدخول", "PRE_ENTRY_STOP_INVALIDATED", "GANN20_PRE_ENTRY_STOP_INVALIDATED", "أُبطلت قبل الدخول", "TERMINAL_FINALIZER", "PRE_ENTRY_STOP_INVALIDATED"),
    }.get(str(state or ""))
    return None if values is None else (values[0], values[1], p50, p100, *values[2:])


def _row_presentation(pulse: _Pulse) -> Tuple[Any, ...]:
    state = str(pulse.state or ""); internal_pending = state == LIVE_INTERNAL_PENDING_SEAL
    live_waiting = state == LIVE_WAITING_R1; live_pending = state in {LIVE_PENDING_SEAL, LIVE_WEAKENING}
    awaiting = state == SEAL_AWAITING_FINAL_SCORE; sealed_p30 = state in {SEALED_P30_CONFIRMED, "SEALED_CONFIRMED"}
    sealed_waiting = state == SEALED_WAITING_R1
    sealed_late_center = state == SEALED_P30_LATE_CENTER_ONLY
    rejected = state == SEALED_REJECTED
    cross_failed_at_seal = state == INTRABAR_CROSS_FAILED_AT_SEAL
    no_chase = state == LIVE_LATE_NO_CHASE
    failed = state == LIVE_FAILED
    expired = state == EXPIRED_SESSION_CLOSE
    target_consumed = state in {TARGET_CONSUMED_BEFORE_ENTRY, TARGET_CONSUMED_BEFORE_OFFICIAL}
    post_r1_presentation = _a101_post_r1_presentation(pulse, state)
    final_score_pending = bool((pulse.audit or {}).get("seal_final_score_pending"))
    seal_failure_reason = str((pulse.audit or {}).get("seal_failure_reason") or (pulse.audit or {}).get("state_reason") or "")
    _target_levels = pulse.sealed_levels if pulse.sealed_levels and pulse.state not in FORMING_STATES else pulse.levels
    target50 = _finite((_target_levels or {}).get("r50_price"), _finite((_target_levels or {}).get("gann20_target50_price")))
    target_reached_before_official = bool(
        target_consumed
        or (math.isfinite(target50) and target50 > 0 and math.isfinite(pulse.signal_bar_high) and _touch(pulse.signal_bar_high, target50, high=pulse.signal_bar_high, market_key=pulse.market_key, symbol=pulse.symbol))
    )
    execution_result = dict((pulse.audit or {}).get("execution_layer_result") or {})
    execution_flat = dict(_flatten_execution_result(execution_result) or {}) if callable(_flatten_execution_result) and execution_result else {}
    execution_passed = bool(execution_result.get("execution_passed"))
    execution_shadow_passed = bool(execution_result.get("execution_shadow_passed", execution_passed))
    execution_authorized = bool(execution_result.get("execution_authorized"))
    execution_hard_blocked = bool(target_reached_before_official or target_consumed or no_chase or failed or expired)
    execution_publishable = bool(
        execution_authorized and execution_passed and not execution_hard_blocked and not final_score_pending
        and state in {LIVE_PENDING_SEAL, SEALED_P30_CONFIRMED, "SEALED_CONFIRMED"}
    )
    # R168 contract: R161 profiles are frozen shadow labels.  Only an
    # explicitly validated authority may promote a pulse to Paper/official.
    publishable = bool(execution_publishable)
    if internal_pending:
        stage = "مراقبة داخلية — أول تقاطع محفوظ"
        status = "داخلي — لا يظهر للمضارب قبل P30 أو ختم 20–30 ثم R1"
        p50_show = pulse.p50_live
        p100_show = pulse.p100_live
        entry_status = "LIVE_INTERNAL_PENDING_SEAL"
        professional_type = "GANN20_RAW_CROSS_INTERNAL"
        visible_case = "مراقبة داخلية"
        truth_scope = "INTRABAR_INTERNAL"
        monitor_outcome = "INTERNAL_PENDING_SEAL"
    elif live_waiting:
        stage = "مراقبة R1 حية — احتمال الميلاد 20–30"
        status = "داخلي — ينتظر تحديث مصدر لاحق بسعر فعلي فوق R1 المجمد؛ لا يعتمد على عدد التكات"
        p50_show = pulse.p50_live
        p100_show = pulse.p100_live
        entry_status = "LIVE_R1_WATCH_INTERNAL"
        professional_type = "GANN20_LIVE_WAITING_R1"
        visible_case = "مراقبة R1 حية"
        truth_scope = "LIVE_BIRTH_R1_WATCH_INTERNAL"
        monitor_outcome = "LIVE_WAITING_R1"
    elif live_pending:
        # H12H14: presentation follows the latest exact live score while the
        # immutable first-cross probability remains available in dedicated fields.
        p50_show = pulse.p50_live_latest if math.isfinite(pulse.p50_live_latest) else pulse.p50_live
        p100_show = pulse.p100_live_latest if math.isfinite(pulse.p100_live_latest) else pulse.p100_live
        truth_scope = "INTRABAR_PROVISIONAL"
        if state == LIVE_WEAKENING:
            stage = "نبضة حية تضعف — التقاطع غير قائم الآن"
            status = "التقاطع اللحظي اختفى قبل الختم؛ قد يعود داخل الشمعة، ولا توجد صلاحية دخول"
            entry_status = "LIVE_WEAKENING_NOT_EXECUTABLE"
            professional_type = "GANN20_LIVE_WEAKENING"
            visible_case = "نبضة تضعف"
            monitor_outcome = "LIVE_WEAKENING"
        else:
            stage = "نبضة P30 حية — قيد الختم"
            status = "نبضة رادار حية — ظهرت عند احتمال نفس لحظة التقاطع/التأهل؛ ليست شراء دون طبقة التنفيذ"
            entry_status = "LIVE_PENDING_SEAL_NOT_EXECUTABLE"
            professional_type = "GANN20_LIVE_PULSE_PENDING_SEAL"
            visible_case = "نبضة حية"
            monitor_outcome = "PENDING_SEAL"
    elif post_r1_presentation is not None:
        stage, status, p50_show, p100_show, entry_status, professional_type, visible_case, truth_scope, monitor_outcome = post_r1_presentation
    elif no_chase:
        stage = "لا تطارد — حالة تنفيذ؛ الظهور محفوظ إذا بلغ النموذج العتبة"
        status = "PAAC Late No Chase — لا تلغي حقيقة تجاوز عتبة النموذج"
        p50_show = pulse.p50_live_latest if math.isfinite(pulse.p50_live_latest) else pulse.p50_live
        p100_show = pulse.p100_live_latest if math.isfinite(pulse.p100_live_latest) else pulse.p100_live
        entry_status = "NO_CHASE_REVIEW_ONLY"
        professional_type = "GANN20_LIVE_LATE_NO_CHASE"
        visible_case = "لا تطارد"
        truth_scope = "TERMINAL_REVIEW_ONLY"
        monitor_outcome = "NO_CHASE"
    elif target_consumed:
        stage = "هدف مستهلك قبل الدخول — لا دخول جديد"
        status = "انتهت — حقق R50 قبل أن يصبح دخولًا صالحًا"
        p50_show = pulse.p50_final if math.isfinite(pulse.p50_final) else pulse.p50_live
        p100_show = pulse.p100_final if math.isfinite(pulse.p100_final) else pulse.p100_live
        entry_status = "TARGET_CONSUMED_BEFORE_ENTRY_NO_NEW_ENTRY"
        professional_type = "GANN20_TARGET_CONSUMED_BEFORE_ENTRY"
        visible_case = "هدف مستهلك"
        truth_scope = "TERMINAL_TARGET_CONSUMED"
        monitor_outcome = "TARGET_CONSUMED_BEFORE_OFFICIAL" if state == TARGET_CONSUMED_BEFORE_OFFICIAL else "TARGET_CONSUMED_BEFORE_ENTRY"
    elif sealed_p30:
        stage = "نبضة P30 مختومة — تأكيد عقد النموذج"
        status = "مختومة P30 — رادار فقط ما لم تجتز طبقة التنفيذ"
        p50_show = pulse.p50_final if math.isfinite(pulse.p50_final) else pulse.p50_live
        p100_show = pulse.p100_final if math.isfinite(pulse.p100_final) else pulse.p100_live
        entry_status = "SEALED_P30_RADAR_ONLY"
        professional_type = "GANN20_SEALED_P30_CONFIRMED"
        visible_case = "نبضة P30 مختومة"
        truth_scope = "SEALED_MODEL_ANCHOR"
        monitor_outcome = "SEALED_P30_CONFIRMED"
    elif sealed_waiting:
        stage = "مراقبة داخلية R1 — مستويات الختم مجمدة"
        status = "داخلي — يظهر مرة واحدة فقط عند اختراق R1 خلال أفق 20 شمعة وقبل التقاطع السلبي"
        p50_show = pulse.p50_final if math.isfinite(pulse.p50_final) else pulse.p50_live
        p100_show = pulse.p100_final if math.isfinite(pulse.p100_final) else pulse.p100_live
        entry_status = "SEALED_R1_WATCH_INTERNAL"
        professional_type = "GANN20_SEALED_WAITING_R1"
        visible_case = "مراقبة R1 داخلية"
        truth_scope = "SEALED_R1_WATCH_INTERNAL"
        monitor_outcome = "SEALED_WAITING_R1"
    elif sealed_late_center:
        stage = "P30 مختومة متأخرة — مركز المنظومات فقط"
        status = "تجاوز الاحتمال العتبة عند الختم لا عند تكة التقاطع؛ ليست إشارة قنص"
        p50_show = pulse.p50_final if math.isfinite(pulse.p50_final) else pulse.p50_live
        p100_show = pulse.p100_final if math.isfinite(pulse.p100_final) else pulse.p100_live
        entry_status = "SEALED_P30_LATE_CENTER_ONLY"
        professional_type = "GANN20_SEALED_P30_LATE_CENTER_ONLY"
        visible_case = "مركز المنظومات"
        truth_scope = "SEALED_MODEL_CENTER_ONLY"
        monitor_outcome = "SEALED_P30_LATE_CENTER_ONLY"
    elif awaiting:
        stage = "انتهت الشمعة — انتظار احتمال الختم الدقيق"
        status = "معلقة داخليًا — غير رسمية حتى اكتمال الختم"
        p50_show = pulse.p50_live
        p100_show = pulse.p100_live
        entry_status = "SEAL_AWAITING_FINAL_SCORE_NOT_PUBLISHABLE"
        professional_type = "GANN20_SEAL_AWAITING_FINAL_SCORE_LEGACY"
        visible_case = "بانتظار الختم النهائي"
        truth_scope = "SEAL_PENDING_INTERNAL_LEGACY"
        monitor_outcome = "LEGACY_FINAL_SCORE_PENDING"
    else:
        stage = "تقاطع لحظي لم يثبت عند إغلاق M30" if cross_failed_at_seal else (
            "انتهت الجلسة — لم تكتمل صلاحية النبضة" if expired else (
                "فشل PAAC — أُلغي الدخول" if failed else (
                    "تعذر الحصول على الدرجة النهائية ضمن نافذة الإعادة" if seal_failure_reason == "final_score_unavailable_retry_window_exhausted" else "ختم ضعيف — لا دخول جديد"
                )
            )
        )
        status = "انتهت — لا إعادة ولا دخول جديد" if cross_failed_at_seal else "انتهت — غير قابلة للدخول"
        p50_show = pulse.p50_final if math.isfinite(pulse.p50_final) else pulse.p50_live
        p100_show = pulse.p100_final if math.isfinite(pulse.p100_final) else pulse.p100_live
        entry_status = "INTRABAR_CROSS_FAILED_AT_SEAL" if cross_failed_at_seal else ("EXPIRED_SESSION_CLOSE" if expired else "REJECTED_AT_SEAL")
        professional_type = "GANN20_INTRABAR_CROSS_FAILED_AT_SEAL" if cross_failed_at_seal else "GANN20_LIVE_PULSE_REJECTED"
        visible_case = "تقاطع لم يثبت" if cross_failed_at_seal else "نبضة لم تُختم"
        truth_scope = "TERMINAL_INTRABAR_CROSS_FAILED" if cross_failed_at_seal else "TERMINAL_REJECTED"
        monitor_outcome = "INTRABAR_CROSS_FAILED_AT_SEAL" if cross_failed_at_seal else ("EXPIRED_SESSION_CLOSE" if expired else "SEALED_REJECTED")
    if execution_publishable:
        stage = "تنفيذ معتمد — سلطة تنفيذ مثبتة"
        status = "نبضة مؤهلة للتنفيذ المعتمد"
        entry_status = str(execution_result.get("entry_status_ar") or "تنفيذ معتمد")
        professional_type = "GANN20_EXECUTION_AUTHORIZED_PULSE"
        visible_case = "تنفيذ معتمد"
        truth_scope = "VALIDATED_EXECUTION_AUTHORITY"
        monitor_outcome = "EXECUTION_AUTHORIZED"
    elif execution_shadow_passed and (live_pending or sealed_p30):
        stage = "R161 ظل — مرشح بحثي غير قابل للشراء"
        status = "اجتاز قواعد R161 القديمة في الظل؛ لا Paper ولا توصية رسمية"
        entry_status = "R161_SHADOW_QUALIFIED_NOT_BUY"
        professional_type = "GANN20_R161_SHADOW_CANDIDATE"
        visible_case = "مرشح تنفيذ ظل"
        truth_scope = "EXECUTION_SHADOW_ONLY"
        monitor_outcome = "R161_SHADOW_QUALIFIED"
    elif sealed_p30 and not final_score_pending:
        # Sealed P30 is a useful radar confirmation, not a money instruction.
        entry_status = "SEALED_RADAR_ONLY_NOT_BUY"
        status = "نبضة مختومة — رادار فقط لا شراء حتى تجتاز طبقة التنفيذ"
        professional_type = "GANN20_SEALED_RADAR_ONLY"
        monitor_outcome = "SEALED_RADAR_ONLY"
    return (
        state, internal_pending, live_waiting, live_pending, awaiting, sealed_p30, sealed_waiting, sealed_late_center, rejected, cross_failed_at_seal, no_chase, failed,
        expired, target_consumed, final_score_pending, seal_failure_reason, target_reached_before_official, execution_result, execution_flat, execution_passed, execution_shadow_passed, execution_authorized, execution_hard_blocked, execution_publishable,
        publishable, stage, status, p50_show, p100_show, entry_status, professional_type, visible_case, truth_scope, monitor_outcome,
    )



_TERMINAL_OUTCOME_PRESENTATION = {
    INTRABAR_CROSS_FAILED_AT_SEAL: ("INTRABAR_CROSS_FAILED_AT_SEAL", "تقاطع لحظي لم يثبت عند الختم"),
    SEALED_REJECTED: ("SEALED_REJECTED", "لم تُختم الإشارة"),
    EXPIRED_SESSION_CLOSE: ("EXPIRED_SESSION_CLOSE", "انتهت الجلسة قبل اكتمال صلاحية الحلقة"),
    LIVE_FAILED: ("LIVE_FAILED", "فشلت النبضة لحظيًا"),
    LIVE_LATE_NO_CHASE: ("LIVE_LATE_NO_CHASE", "لا تطارد — أغلقت المتابعة"),
    TARGET_CONSUMED_BEFORE_ENTRY: ("TARGET_CONSUMED_BEFORE_ENTRY", "تحقق الهدف قبل دخول صالح"),
    TARGET_CONSUMED_BEFORE_OFFICIAL: ("TARGET_CONSUMED_BEFORE_OFFICIAL", "تحقق الهدف قبل الاعتماد الرسمي"),
    R100_HIT_COMPLETE: ("R100_HIT", "R100 تحقق — الحلقة مكتملة"),
    EPISODE_EXPIRED_20_BARS: ("EXPIRED_20_BARS", "انتهت 20 شمعة"),
    EPISODE_CLOSED_NEGATIVE_CROSS: ("CLOSED_NEGATIVE_CROSS", "أغلقت الحلقة بتقاطع سلبي"),
    PRE_ENTRY_STOP_INVALIDATED: ("PRE_ENTRY_STOP_INVALIDATED", "ضُرب الوقف قبل الدخول"),
}


def _sealed_probability_levels(final_prob: Mapping[str, Any], final_obs: Mapping[str, Any], bar_time: Any, probability_asof: Any) -> Dict[str, Any]:
    out = {
        key: final_prob.get(key)
        for key in ("anchor_price", "r1_price", "r50_price", "r100_price", "stop_price", "model_version")
        if final_prob.get(key) not in (None, "")
    }
    out.update({
        "probability_kind": final_prob.get("probability_kind") or PROB_SCOPE_SEALED_CROSS_BAR,
        "probability_scope": PROB_SCOPE_SEALED_CROSS_BAR,
        "probability_asof": probability_asof,
        "probability_bar_time": str(bar_time or ""),
        "probability_source_observation_id": str(final_prob.get("probability_source_observation_id") or final_obs.get("source_observation_id") or ""),
    })
    return out


def _probability_display_truth(pulse: _Pulse, levels: Mapping[str, Any], sealed_levels: Mapping[str, Any], p50_show: Any, probability_kind: str) -> Tuple[bool, Any, Any, str]:
    available = bool(math.isfinite(pulse.p50_final))
    if available and math.isfinite(p50_show) and abs(float(p50_show) - float(pulse.p50_final)) <= 1e-9:
        return (
            True,
            pulse.probability_scope_sealed or PROB_SCOPE_SEALED_CROSS_BAR,
            pulse.probability_asof_sealed or pulse.sealed_at or None,
            str(sealed_levels.get("probability_kind") or probability_kind or "MARKET_WIDE_CONFIRMED"),
        )
    return (
        available,
        pulse.probability_scope_first_cross or pulse.probability_scope_live or None,
        pulse.probability_asof_first_cross or pulse.probability_asof_live or None,
        str(levels.get("probability_kind") or probability_kind or "PARTIAL_MARKET_PROVISIONAL"),
    )


def _probability_label(pulse: _Pulse, p50_show: Any, p100_show: Any, sealed_available: bool) -> str:
    if math.isfinite(pulse.p50_at_first_cross) and sealed_available:
        return f"ميلاد R50 {pulse.p50_at_first_cross:.1f}% | ختم {pulse.p50_final:.1f}%"
    if math.isfinite(p50_show) and not sealed_available:
        return f"أول تقاطع R50 {p50_show:.1f}% | R100 {p100_show:.1f}%"
    return f"R50 {p50_show:.1f}% | R100 {p100_show:.1f}%" if math.isfinite(p50_show) else "-"


def _probability_display_row_fields(available: bool, scope: Any, asof: Any, kind: str) -> Dict[str, Any]:
    return {
        "probability_scope": scope,
        "probability_display_scope": scope,
        "probability_display_kind": kind,
        "probability_asof": asof,
        "sealed_probability_available": available,
    }


def _restore_probability_lineage(row: Dict[str, Any], pulse: _Pulse, sealed_levels: Mapping[str, Any], available: bool, scope: Any, asof: Any, kind: str) -> Dict[str, Any]:
    row.update(_probability_display_row_fields(available, scope, asof, kind))
    row["gann20_probability_kind"] = kind
    if available:
        row["probability_source_observation_id"] = sealed_levels.get("probability_source_observation_id") or row.get("probability_source_observation_id")
        row["probability_bar_time"] = sealed_levels.get("probability_bar_time") or pulse.bar_time
    return row

def _a101_initialize_engine(engine: Any, journal_root: Optional[Path]) -> None:
    engine._lock = threading.RLock()
    engine._by_symbol = {}; engine._by_id = {}
    engine._journal_root = Path(journal_root) if journal_root is not None else _journal_root()
    engine._transition_event_signatures = set()
    engine._transition_receipts = {}
    engine._durable_terminal_by_alias = {}
    engine._pending_terminal_intent_by_alias = {}
    engine._terminal_intent_reconciliation = {"attempted": 0, "committed": 0, "failed": 0, "errors": []}
    # Acceptance supplies one shared SessionRuntimeClone root.  Standalone
    # engines use their own journal root, preventing cross-test/tool leakage.
    if str(os.environ.get("AIN_TERMINAL_TRUTH_DB_PATH") or "").strip() or str(os.environ.get("AIN_PULSE_TICK_TAPE_DIR") or "").strip() or str(os.environ.get("AIN_RUNTIME_SESSION_DIR") or "").strip():
        from terminal_truth_authority import terminal_truth_db_path as _terminal_truth_db_path
        engine._terminal_truth_path = _terminal_truth_db_path()
    else:
        from terminal_truth_authority import terminal_truth_db_path as _terminal_truth_db_path
        engine._terminal_truth_path = _terminal_truth_db_path(engine._journal_root)
    # Reconcile terminal write-ahead intents before loading authority.  These
    # files exist only for terminal transitions whose SQLite COMMIT did not
    # complete (or whose process died around COMMIT); ordinary observations and
    # LIVE_UPDATE ticks never enter this path.
    from terminal_truth_authority import (
        reconcile_terminal_transition_intents as _reconcile_terminal_transition_intents,
        pending_terminal_transition_intents as _pending_terminal_transition_intents,
    )
    try:
        engine._terminal_intent_reconciliation = dict(
            _reconcile_terminal_transition_intents(path=engine._terminal_truth_path) or {}
        )
    except Exception as exc:
        # A corrupt/unreadable intent cannot be safely mapped to an Episode veto.
        # Refuse engine construction instead of silently permitting duplicate
        # births whose prior terminal proposal may have survived a crash.
        raise RuntimeError(
            f"TERMINAL_INTENT_RECONCILIATION_FAILED:{type(exc).__name__}:{exc}"
        ) from exc
    # If authority remains unavailable, pending intents still veto a duplicate
    # birth.  They do not claim to be Terminal truth; they preserve the retry
    # debt until the authority can be reconciled.
    try:
        for intent in _pending_terminal_transition_intents(path=engine._terminal_truth_path):
            intent_row = dict(intent or {})
            aliases = tuple(intent_row.get("aliases") or ()) or (str(intent_row.get("episode_id") or ""),)
            for alias in aliases:
                alias_text = str(alias or "").strip()
                if alias_text:
                    engine._pending_terminal_intent_by_alias[alias_text] = intent_row
    except Exception as exc:
        raise RuntimeError(
            f"TERMINAL_INTENT_LOAD_FAILED:{type(exc).__name__}:{exc}"
        ) from exc

    # Terminal truth is loaded once per engine generation.  The hot observation
    # path consults this in-memory authority map; it does not hit SQLite per tick.
    for raw in _load_terminal_truths(path=engine._terminal_truth_path):
        row = dict(raw or {})
        aliases = tuple(row.get("episode_aliases") or ()) or (str(row.get("episode_id") or ""),)
        for alias in aliases:
            alias_text = str(alias or "").strip()
            if alias_text:
                engine._durable_terminal_by_alias[alias_text] = row


def _a101_export_engine_state(engine: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for pulse in engine._by_id.values():
        if pulse.state in PERSISTED_ACTIVE_STATES:
            payload = asdict(pulse); payload["last_emit_signature"] = list(pulse.last_emit_signature or ())
            rows.append({"version": VERSION, "pulse": payload})
    rows.extend({"version": VERSION, "transition_receipt": dict(receipt)} for receipt in engine._transition_receipts.values())
    rows.sort(key=lambda row: (
        0 if "pulse" in row else 1,
        str((row.get("pulse") or row.get("transition_receipt") or {}).get("market_key") or ""),
        str((row.get("pulse") or row.get("transition_receipt") or {}).get("bar_time") or ""),
        str((row.get("pulse") or row.get("transition_receipt") or {}).get("pulse_id") or (row.get("transition_receipt") or {}).get("transition_id") or ""),
    ))
    return rows


def _a101_restore_engine_state(engine: Any, rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    restored = skipped = receipts_restored = 0
    allowed = {item.name for item in fields(_Pulse)}
    for item in rows or []:
        item_map = dict(item or {}); receipt = dict(item_map.get("transition_receipt") or {})
        transition_id = str(receipt.get("transition_id") or "").strip()
        if transition_id:
            engine._transition_receipts.setdefault(transition_id, receipt); receipts_restored += 1; continue
        raw = dict(item_map.get("pulse") or item_map or {})
        if str(raw.get("state") or "") not in PERSISTED_ACTIVE_STATES:
            skipped += 1; continue
        pulse_id = str(raw.get("pulse_id") or "")
        if not pulse_id or pulse_id in engine._by_id:
            skipped += 1; continue
        values = {key: value for key, value in raw.items() if key in allowed}
        values["last_emit_signature"] = tuple(values.get("last_emit_signature") or ())
        try:
            pulse = _Pulse(**values)
        except Exception:
            skipped += 1; continue
        pulse.audit = dict(pulse.audit or {}); pulse.audit["restored_from_state_store"] = True
        engine._by_id[pulse.pulse_id] = pulse; key = engine._key(pulse.market_key, pulse.symbol)
        existing = engine._by_symbol.get(key)
        if existing is None or (_parse_bar_time(pulse.bar_time, market_key=pulse.market_key) or _dt.datetime.min) >= (_parse_bar_time(existing.bar_time, market_key=existing.market_key) or _dt.datetime.min):
            engine._by_symbol[key] = pulse
        restored += 1
    engine._prune_resolved()
    return {"restored": restored, "skipped": skipped, "receipts_restored": receipts_restored}


def _a101_apply_atomic_state_owned(
    engine: Any, pulse: _Pulse, state: str, now_s: str, *, reason: str = "",
    event_type: str = "state_transition", extra: Optional[Dict[str, Any]] = None,
    transition_id: str = "",
) -> Dict[str, Any]:
    target, details = str(state or ""), dict(extra or {})
    applied_ids = list((pulse.audit or {}).get("applied_transition_ids") or [])
    if transition_id and transition_id in applied_ids:
        return engine._row(pulse, event_type=event_type)
    signature = (pulse.pulse_id, target, str(event_type or ""), str(reason or ""), str(transition_id or ""))
    if pulse.state == target and signature in engine._transition_event_signatures:
        return engine._row(pulse, event_type=event_type)

    terminal = target in TERMINAL_STATES
    working = _clone_pulse(pulse) if terminal else pulse
    working.state, working.last_seen_at = target, now_s
    if target in TERMINAL_STATES or target == SEAL_AWAITING_FINAL_SCORE:
        working.sealed_at = working.sealed_at or now_s
    working.audit = dict(working.audit or {})
    if reason:
        working.audit["state_reason"] = reason
    if transition_id:
        working.audit["last_transition_id"] = transition_id
    journal_extra = {"reason": reason, "transition_id": transition_id or None, **details}
    if terminal:
        _commit_terminal_proposal(engine, pulse, working, event_type, journal_extra)
    else:
        engine._journal(working, event_type, journal_extra)

    row = engine._row(pulse, event_type=event_type)
    episode_event = str(details.get("episode_event_type") or target or event_type).upper()
    event_origin = str(details.get("event_origin") or row.get("truth_source") or engine._truth_source_for_state(target))
    _append_episode_event(
        episode_event, row,
        extra={"event_origin": event_origin, "reason_code": reason or episode_event, "transition_id": transition_id or None},
        suppress_duplicate=True,
    )
    if transition_id:
        applied_ids.append(transition_id)
        pulse.audit["applied_transition_ids"] = applied_ids[-256:]
    engine._transition_event_signatures.add(signature)
    return row



def _a101_terminate_engine_episode(
    engine: Any, pulse_id: str, *, state: str, at: Any = None, reason: str = "", transition_id: str = "",
) -> Optional[Dict[str, Any]]:
    terminal = str(state or "").upper()
    if terminal not in TERMINAL_STATES:
        raise ValueError(f"Non-terminal lifecycle state: {terminal}")
    now_s = _iso(at)
    if transition_id:
        acknowledged = engine._receipt_ack_row(transition_id)
        if acknowledged is not None:
            return acknowledged
    pulse = engine._by_id.get(str(pulse_id or ""))
    if pulse is None:
        return None
    if pulse.state in TERMINAL_STATES:
        authority = dict(_terminal_truth_for_episode(pulse.pulse_id, path=engine._terminal_truth_path) or {})
        if not bool(authority.get("terminal_truth_durable")):
            # A legacy/partial in-memory Terminal value is not an ACK receipt.
            # Returning terminal_snapshot here caused candidate deletion and
            # rebirth after restart.  Keep the transition pending instead.
            raise RuntimeError(
                f"TERMINAL_SNAPSHOT_WITHOUT_DURABLE_AUTHORITY:{pulse.pulse_id}:{pulse.state}"
            )
        row = engine._row(pulse, event_type="terminal_snapshot")
        row["terminal_truth_durable"] = True
        row["terminal_authority_episode_id"] = authority.get("episode_id")
    else:
        row = engine._apply_atomic_state(
            pulse, terminal, now_s, reason=reason or terminal,
            event_type=terminal, transition_id=transition_id,
            extra={"event_origin": TRUTH_TERMINAL, "episode_event_type": terminal},
        )
    authority = dict(_terminal_truth_for_episode(pulse.pulse_id, path=engine._terminal_truth_path) or {})
    if not bool(authority.get("terminal_truth_durable")):
        raise RuntimeError(f"TERMINAL_TRANSITION_WITHOUT_DURABLE_AUTHORITY:{pulse.pulse_id}:{terminal}")
    engine._remember_transition_receipt(
        transition_id, pulse=pulse, state=pulse.state, truth_source=TRUTH_TERMINAL,
        truth_rank=60, at=now_s, authoritative_row=row,
    )
    return row



class LivePulseSealEngine:
    def __init__(self, *, journal_root: Optional[Path] = None) -> None:
        _a101_initialize_engine(self, journal_root)

    @staticmethod
    def _key(market_key: str, symbol: str) -> Tuple[str, str]:
        return (str(market_key or ""), str(symbol or "").strip().upper())

    @staticmethod
    def _pulse_id(market_key: str, symbol: str, bar_time: str) -> str:
        return _canonical_episode_id(market_key, symbol, bar_time)

    @staticmethod
    def _truth_source_for_state(state: str) -> str:
        value = str(state or "").upper()
        if value in TERMINAL_STATES: return TRUTH_TERMINAL
        if value in ACTIVE_POST_R1_STATES: return TRUTH_R1_ACTIVATION
        if value.startswith("SEALED_") or value == SEAL_AWAITING_FINAL_SCORE:
            return TRUTH_SEALED
        if value in FORMING_STATES:
            return TRUTH_LIVE_SOURCE
        return TRUTH_LIFECYCLE

    def unresolved_truths(self, *, market_key: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return every unresolved episode for the independent time owner."""
        market = None if market_key is None else str(market_key or "")
        with self._lock:
            pulses = [
                pulse for pulse in self._by_id.values()
                if pulse.state in PERSISTED_ACTIVE_STATES
                and (market is None or str(pulse.market_key or "") == market)
            ]
            pulses.sort(key=lambda pulse: (pulse.market_key, pulse.bar_time, pulse.symbol, pulse.pulse_id))
            return [self._row(pulse, event_type="unresolved_snapshot") for pulse in pulses]

    def _receipt_ack_row(self, transition_id: str) -> Optional[Dict[str, Any]]:
        receipt = dict(self._transition_receipts.get(str(transition_id or "")) or {})
        if not receipt:
            return None
        row = dict(receipt.get("authoritative_row") or {})
        row.update({
            "version": VERSION,
            "transition_acknowledged": True,
            "transition_replayed_from_receipt": True,
            "transition_id": str(transition_id or ""),
            "episode_id": receipt.get("episode_id"),
            "pulse_episode_id": receipt.get("episode_id"),
            "state": receipt.get("state"),
            "live_pulse_seal_state": receipt.get("state"),
            "truth_source": receipt.get("truth_source"),
            "truth_rank": receipt.get("truth_rank"),
            "receipt_recorded_at": receipt.get("recorded_at"),
        })
        return row

    def _remember_transition_receipt(
        self, transition_id: str, *, pulse: _Pulse, state: str, truth_source: str, truth_rank: int, at: Any,
        authoritative_row: Optional[Mapping[str, Any]] = None,
    ) -> None:
        wanted = str(transition_id or "").strip()
        if not wanted:
            return
        self._transition_receipts[wanted] = {
            "transition_id": wanted,
            "episode_id": pulse.pulse_id,
            "state": str(state or pulse.state or ""),
            "truth_source": str(truth_source or self._truth_source_for_state(state)),
            "truth_rank": int(truth_rank),
            "recorded_at": _iso(at),
            "authoritative_row": dict(authoritative_row or {}),
        }
        # Bound durable receipt debt while retaining a generous restart window.
        if len(self._transition_receipts) > 10000:
            ordered = sorted(
                self._transition_receipts.items(),
                key=lambda item: str((item[1] or {}).get("recorded_at") or ""),
            )
            self._transition_receipts = dict(ordered[-10000:])

    def export_unresolved_state(self) -> List[Dict[str, Any]]:
        with self._lock: return _a101_export_engine_state(self)

    def restore_unresolved_state(self, rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
        with self._lock: return _a101_restore_engine_state(self, rows)

    def apply_external_transition(
        self, pulse_id: str, *, state: str, at: Any = None, reason: str = "",
        truth_source: str = TRUTH_R1_ACTIVATION, truth_rank: int = 55,
        audit_patch: Optional[Mapping[str, Any]] = None, transition_id: str = "",
        transition_seq: int = 0, previous_state: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Apply one causally ordered tick-owned transition without truth regression."""
        target = str(state or "").upper()
        allowed = {*ACTIVE_POST_R1_STATES, R100_HIT_COMPLETE}
        if target not in allowed:
            raise ValueError(f"Unsupported external lifecycle state: {target}")
        now_s = _iso(at)
        with self._lock:
            if transition_id:
                acknowledged = self._receipt_ack_row(transition_id)
                if acknowledged is not None:
                    return acknowledged
            pulse = self._by_id.get(str(pulse_id or ""))
            if pulse is None:
                return None
            source = str(truth_source or (TRUTH_TERMINAL if target in TERMINAL_STATES else TRUTH_R1_ACTIVATION))
            rank = int(truth_rank)
            current_rank = int(pulse.audit.get("truth_rank") or (60 if pulse.state in TERMINAL_STATES else 0))
            last_seq = int(pulse.audit.get("last_lifecycle_transition_seq") or 0)
            seq = int(transition_seq or 0)
            expected_previous = str(previous_state or "").upper()

            def _ack_obsolete(code: str) -> Dict[str, Any]:
                row = self._row(pulse, event_type="obsolete_lifecycle_transition")
                row.update({
                    "transition_acknowledged": True, "transition_obsolete": True,
                    "transition_obsolete_reason": code, "transition_id": transition_id or None,
                    "transition_seq": seq or None, "requested_state": target,
                })
                self._remember_transition_receipt(
                    transition_id, pulse=pulse, state=pulse.state,
                    truth_source=str(pulse.audit.get("truth_source") or self._truth_source_for_state(pulse.state)),
                    truth_rank=current_rank, at=now_s, authoritative_row=row,
                )
                return row

            # Terminal truth and higher-rank truth are immutable. A stale message is
            # acknowledged as obsolete so it cannot poison the outbox forever.
            if pulse.state in TERMINAL_STATES:
                return _ack_obsolete("TERMINAL_STATE_IMMUTABLE")
            if current_rank > rank:
                return _ack_obsolete("TRUTH_RANK_REGRESSION_BLOCKED")
            if seq and seq <= last_seq:
                return _ack_obsolete("TRANSITION_SEQUENCE_ALREADY_PASSED")
            if expected_previous and pulse.state != expected_previous:
                if pulse.state == target:
                    return _ack_obsolete("TARGET_ALREADY_AUTHORITATIVE")
                return _ack_obsolete("PREVIOUS_STATE_MISMATCH")

            pulse.audit.update(dict(audit_patch or {}))
            pulse.audit["truth_source"] = source
            pulse.audit["truth_rank"] = rank
            pulse.audit["r1_lifecycle_owned_by_tick_tape"] = True
            if seq:
                pulse.audit["last_lifecycle_transition_seq"] = seq
            event_origin = TRUTH_TERMINAL if target in TERMINAL_STATES else TRUTH_R1_ACTIVATION
            row = self._apply_atomic_state(
                pulse, target, now_s, reason=reason or target,
                event_type="r1_lifecycle_transition", transition_id=transition_id,
                extra={"event_origin": event_origin, "episode_event_type": target},
            )
            self._remember_transition_receipt(
                transition_id, pulse=pulse, state=target, truth_source=source, truth_rank=rank, at=now_s,
                authoritative_row=row,
            )
            return row

    def terminate_episode(self, pulse_id: str, *, state: str, at: Any = None, reason: str = "", transition_id: str = "") -> Optional[Dict[str, Any]]:
        with self._lock: return _a101_terminate_engine_episode(self, pulse_id, state=state, at=at, reason=reason, transition_id=transition_id)

    def truth_for_symbol(self, market_key: str, symbol: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            pulse = self._by_symbol.get(self._key(market_key, symbol))
            return self._row(pulse, event_type="snapshot") if pulse is not None else None

    def active_truths_for_market(self, market_key: str) -> List[Dict[str, Any]]:
        """Return current symbol pointers for causal decision-context counts.

        This is a snapshot for SHADOW research only.  It does not authorize or
        block publication and deliberately excludes expired/rejected rows.
        """
        market = str(market_key or "")
        with self._lock:
            pulses = []
            for (key_market, _symbol), pulse in self._by_symbol.items():
                if str(key_market) != market:
                    continue
                row = self._row(pulse, event_type="decision_context_snapshot")
                if _taxonomy_is_terminal(row):
                    continue
                pulses.append(pulse)
            return [self._row(pulse, event_type="decision_context_snapshot") for pulse in pulses]

    def active_counts_for_market(self, market_key: str, *, signal_bar_time: str = "") -> Dict[str, int]:
        return _taxonomy_active_counts(
            self.active_truths_for_market(market_key),
            market_key=market_key, signal_bar_time=signal_bar_time,
        )

    def truth_for_pulse_id(self, pulse_id: str) -> Optional[Dict[str, Any]]:
        """Return one exact episode, including an older episode awaiting seal."""
        with self._lock:
            pulse = self._by_id.get(str(pulse_id or ""))
            return self._row(pulse, event_type="snapshot_by_id") if pulse is not None else None

    def truth_for_bar(self, market_key: str, symbol: str, bar_time: str) -> Optional[Dict[str, Any]]:
        """Return the exact LP episode for a symbol/bar, even after a newer bar owns the symbol pointer."""
        pulse_id = self._pulse_id(market_key, symbol, str(bar_time or ""))
        return self.truth_for_pulse_id(pulse_id)

    def pending_seals_for_symbol(self, market_key: str, symbol: str, *, limit: int = 8) -> List[Dict[str, Any]]:
        """Return unresolved closed-bar episodes so the source path can retry sealing.

        The newest active pulse remains available through ``truth_for_symbol``.  This
        method exposes only older episodes whose bar already ended but whose exact
        final probability was temporarily unavailable.  They are never official
        while waiting.
        """
        key = self._key(market_key, symbol)
        with self._lock:
            pending = [
                pulse for pulse in self._by_id.values()
                if self._key(pulse.market_key, pulse.symbol) == key
                and pulse.state in FINAL_SCORE_RETRY_STATES and bool(pulse.audit.get("seal_final_score_pending", True))
            ]
            pending.sort(key=lambda pulse: (pulse.bar_time, pulse.pulse_id))
            if limit > 0:
                pending = pending[-int(limit):]
            return [self._row(pulse, event_type="pending_seal_retry") for pulse in pending]

    def _prune_resolved(self, max_items: int = 10000) -> None:
        """Bound retained history without ever dropping unresolved seal work."""
        if len(self._by_id) <= max_items:
            return
        resolved = [
            pulse for pulse in self._by_id.values()
            if pulse.state in TERMINAL_STATES
            and not (pulse.state in FINAL_SCORE_RETRY_STATES and bool(pulse.audit.get("seal_final_score_pending", False)))
        ]
        resolved.sort(key=lambda pulse: (pulse.sealed_at or pulse.last_seen_at or pulse.appearance_at, pulse.pulse_id))
        for pulse in resolved[: max(0, len(self._by_id) - max_items)]:
            if self._by_symbol.get(self._key(pulse.market_key, pulse.symbol)) is pulse:
                continue
            self._by_id.pop(pulse.pulse_id, None)

    def _expire_excess_pending_for_symbol(self, key: Tuple[str, str]) -> None:
        """Fail closed on pending work older than the retry window.

        ``pending_seals_for_symbol`` retries the latest eight exact bars.  Keeping
        older unresolved objects forever would create an unbounded memory leak while
        those bars can no longer be retried by the source path.  They are therefore
        converted to an explicit non-official rejection and journaled, never silently
        dropped or promoted.
        """
        pending = [
            pulse for pulse in self._by_id.values()
            if self._key(pulse.market_key, pulse.symbol) == key
            and pulse.state in FINAL_SCORE_RETRY_STATES and bool(pulse.audit.get("seal_final_score_pending", True))
        ]
        pending.sort(key=lambda pulse: (pulse.bar_time, pulse.pulse_id))
        overflow = pending[:-MAX_PENDING_SEALS_PER_SYMBOL]
        for pulse in overflow:
            proposal = _clone_pulse(pulse)
            proposal.state = SEALED_REJECTED
            proposal.sealed_at = _iso()
            proposal.last_seen_at = proposal.sealed_at
            proposal.audit = dict(proposal.audit or {})
            proposal.audit["seal_failure_reason"] = "final_score_unavailable_retry_window_exhausted"
            proposal.audit["seal_final_score_pending"] = False
            _commit_terminal_proposal(
                self, pulse, proposal, "seal_score_unavailable_expired",
                {"reason": proposal.audit["seal_failure_reason"]},
            )
        if overflow:
            self._prune_resolved()

    def _journal(self, pulse: _Pulse, event_type: str, extra: Optional[Dict[str, Any]] = None) -> None:
        # A4: publish a shared terminal veto *before* any downstream candidate
        # registration can observe this transition.  This closes the race where
        # LivePulseSeal became terminal and TickTape recreated LIVE_WAITING_R1 a
        # few hundred milliseconds later.
        if pulse.state in TERMINAL_STATES:
            try:
                from pulse_tick_tape import note_terminal_episode as _note_terminal_episode
                _projection_row = self._row(pulse, event_type=str(event_type or pulse.state))
                _projection_row.update({
                    **dict(extra or {}),
                    "episode_id": pulse.pulse_id, "pulse_episode_id": pulse.pulse_id,
                    "market_key": pulse.market_key, "symbol": pulse.symbol,
                    "signal_bar_time": pulse.bar_time,
                    "terminal_state": pulse.state, "live_pulse_seal_state": pulse.state,
                    "gann20_episode_state": pulse.state, "action_state": pulse.state,
                    "sealed_at": pulse.sealed_at or pulse.last_seen_at or _iso(),
                    "sealed_truth_id": (pulse.audit or {}).get("sealed_truth_id"),
                    "terminal_truth_durable": True,
                    "terminal_projection_pending": True,
                    "ui_patch_required": True,
                    "live_publishable": False,
                    "tradable": False,
                    "execution_authorized": False,
                })
                _veto_receipt = _note_terminal_episode(
                    pulse.pulse_id, pulse.state, market_key=pulse.market_key, symbol=pulse.symbol,
                    at=pulse.sealed_at or pulse.last_seen_at or _iso(),
                    reason=str((extra or {}).get("reason") or (pulse.audit or {}).get("state_reason") or event_type or pulse.state),
                    row=_projection_row,
                    authority_path=self._terminal_truth_path,
                )
                pulse.audit["terminal_veto_recorded"] = bool((_veto_receipt or {}).get("recorded"))
                pulse.audit["terminal_truth_durable"] = bool((_veto_receipt or {}).get("authority_durable") or (_veto_receipt or {}).get("terminal_truth_durable"))
                pulse.audit["terminal_veto_error"] = (_veto_receipt or {}).get("projection_persist_error") or None
                if not pulse.audit["terminal_truth_durable"]:
                    raise RuntimeError("TERMINAL_TRUTH_AUTHORITY_NOT_DURABLE")
                durable_row = {
                    "episode_id": pulse.pulse_id,
                    "episode_aliases": list(((_veto_receipt or {}).get("episode_aliases") or (pulse.pulse_id,))),
                    "terminal_state": pulse.state,
                    "terminal_at": pulse.sealed_at or pulse.last_seen_at or _iso(),
                    "payload_hash": (_veto_receipt or {}).get("terminal_truth_payload_hash"),
                }
                for alias in durable_row["episode_aliases"]:
                    alias_text = str(alias or "").strip()
                    if alias_text:
                        self._durable_terminal_by_alias[alias_text] = durable_row
            except Exception as exc:
                pulse.audit["terminal_veto_recorded"] = False
                pulse.audit["terminal_veto_error"] = f"{type(exc).__name__}:{exc}"
                _record_stage_error(
                    "event_write", "live_pulse_terminal_veto", exc,
                    market=str(pulse.market_key), symbol=str(pulse.symbol), episode_id=str(pulse.pulse_id),
                    source_observation_id=str((pulse.audit or {}).get("source_observation_id") or ""),
                    reason_code="LIVE_PULSE_TERMINAL_VETO_FAILED_CLOSED",
                )
                raise
        day = (pulse.appearance_at or _iso())[:10]; path = self._journal_root / day / f"{_safe_slug(pulse.market_key)}_live_pulse_seal.jsonl"
        target50 = _finite(((pulse.sealed_levels if pulse.sealed_levels and pulse.state not in FORMING_STATES else pulse.levels) or {}).get("r50_price"), _finite((pulse.levels or {}).get("gann20_target50_price")))
        target_reached_before_official = bool(pulse.state in {TARGET_CONSUMED_BEFORE_ENTRY, TARGET_CONSUMED_BEFORE_OFFICIAL} or (math.isfinite(target50) and target50 > 0 and math.isfinite(pulse.signal_bar_high) and pulse.signal_bar_high >= target50))
        payload = {
            "version": VERSION,
            "event_type": event_type,
            "pulse_id": pulse.pulse_id,
            "pulse_episode_id": pulse.pulse_id,
            "gann20_contract_version": GANN20_CONTRACT_VERSION,
            "market_key": pulse.market_key,
            "symbol": pulse.symbol,
            "bar_time": pulse.bar_time,
            "first_cross_bar": pulse.bar_time,
            "first_cross_at": pulse.first_cross_at,
            "first_cross_price": pulse.first_cross_price,
            "state": pulse.state,
            "gann20_episode_state": pulse.state,
            "appearance_at": pulse.appearance_at,
            "appearance_price": pulse.appearance_price,
            "p50_at_first_cross": pulse.p50_at_first_cross if math.isfinite(pulse.p50_at_first_cross) else None,
            "p100_at_first_cross": pulse.p100_at_first_cross if math.isfinite(pulse.p100_at_first_cross) else None,
            "p50_at_qualification": pulse.p50_at_qualification if math.isfinite(pulse.p50_at_qualification) else None,
            "p100_at_qualification": pulse.p100_at_qualification if math.isfinite(pulse.p100_at_qualification) else None,
            "p50_birth": pulse.p50_live if math.isfinite(pulse.p50_live) else None, "p100_birth": pulse.p100_live if math.isfinite(pulse.p100_live) else None,
            "p50_live": pulse.p50_live if math.isfinite(pulse.p50_live) else None,
            "p100_live": pulse.p100_live if math.isfinite(pulse.p100_live) else None,
            "p50_live_latest_audit": pulse.p50_live_latest if math.isfinite(pulse.p50_live_latest) else None, "p100_live_latest_audit": pulse.p100_live_latest if math.isfinite(pulse.p100_live_latest) else None,
            "p50_final": pulse.p50_final if math.isfinite(pulse.p50_final) else None,
            "p100_final": pulse.p100_final if math.isfinite(pulse.p100_final) else None,
            "probability_scope_first_cross": pulse.probability_scope_first_cross or None,
            "probability_at_cross_exact": bool(math.isfinite(pulse.p50_at_first_cross)),
            "source_observation_id": pulse.live_sniper_source_observation_id or (pulse.audit or {}).get("source_observation_id"),
            "probability_source_observation_id": pulse.live_sniper_source_observation_id or None,
            "probability_bar_time": pulse.bar_time if math.isfinite(pulse.p50_at_first_cross) else None,
            "probability_scope_live": pulse.probability_scope_live or None,
            "probability_scope_sealed": pulse.probability_scope_sealed or None,
            "threshold_pct": pulse.threshold_pct,
            "signal_bar_close": (pulse.signal_bar_close if ((pulse.state in TERMINAL_STATES or pulse.state in {SEALED_P30_CONFIRMED, SEALED_WAITING_R1, SEALED_P30_LATE_CENTER_ONLY}) and math.isfinite(pulse.signal_bar_close)) else None),
            "signal_bar_high": (pulse.signal_bar_high if math.isfinite(pulse.signal_bar_high) else None),
            "target_reached_before_official": target_reached_before_official,
            "sealed_at": pulse.sealed_at or None,
            "sealed_model_anchor_at": pulse.sealed_model_anchor_at or None,
            "sealed_model_anchor_price": pulse.sealed_model_anchor_price if math.isfinite(pulse.sealed_model_anchor_price) else None,
            "model_horizon_bars": HORIZON_BARS,
            "model_bars_elapsed": int(pulse.model_bars_elapsed or 0),
            "model_bars_remaining": max(0, HORIZON_BARS - int(pulse.model_bars_elapsed or 0)),
            "source_mtime_ns": pulse.source_mtime_ns,
            "execution_decision": (pulse.audit or {}).get("execution_layer_result", {}).get("execution_decision") if isinstance((pulse.audit or {}).get("execution_layer_result"), Mapping) else None,
            "execution_passed": (pulse.audit or {}).get("execution_layer_result", {}).get("execution_passed") if isinstance((pulse.audit or {}).get("execution_layer_result"), Mapping) else None,
            "execution_tier": (pulse.audit or {}).get("execution_layer_result", {}).get("execution_tier") if isinstance((pulse.audit or {}).get("execution_layer_result"), Mapping) else None,
            **dict(extra or {}),
        }
        payload = _stamp_episode_truth(payload, truth_source=self._truth_source_for_state(pulse.state), producer_contract_version=VERSION); payload.setdefault("event_origin", payload.get("truth_source"))
        try: _append_jsonl(path, payload)
        except Exception as exc:
            _record_stage_error(
                "event_write", "live_pulse_journal_append", exc,
                market=str(pulse.market_key), symbol=str(pulse.symbol),
                episode_id=str(pulse.pulse_id),
                source_observation_id=str((pulse.audit or {}).get("source_observation_id") or ""),
                reason_code="LIVE_PULSE_JOURNAL_APPEND_FAILED",
            )

    def _apply_atomic_state(self, pulse: _Pulse, state: str, now_s: str, **kwargs: Any) -> Dict[str, Any]:
        return self._apply_atomic_state_owned(pulse, state, now_s, **kwargs)

    def _apply_atomic_state_owned(self, pulse: _Pulse, state: str, now_s: str, **kwargs: Any) -> Dict[str, Any]:
        return _a101_apply_atomic_state_owned(self, pulse, state, now_s, **kwargs)

    def _apply_level_truth(self, pulse: _Pulse, obs: Mapping[str, Any], now_s: str) -> Dict[str, Any]:
        levels_for_truth = dict(pulse.levels or {})
        # R1 belongs to the sealed 20-30 strategy.  Provisional live geometry may
        # be useful for R50 consumption diagnostics, but it must never arm/trigger
        # R1 before the exact cross bar seal chooses SEALED_WAITING_R1.
        if pulse.state != SEALED_WAITING_R1:
            levels_for_truth["r1_price"] = None
            levels_for_truth["gann20_breakout_price"] = None
        truth = _level_truth(levels_for_truth, obs or {}, market_key=pulse.market_key, symbol=pulse.symbol)
        if math.isfinite(truth.get("high", float("nan"))):
            pulse.signal_bar_high = max(_finite(pulse.signal_bar_high, truth["high"]), truth["high"])
        if math.isfinite(truth.get("price", float("nan"))):
            pulse.current_price = truth["price"]
        if truth.get("activated_r1"):
            pulse.audit.setdefault("activated_r1", True)
            pulse.audit.setdefault("first_r1_crossed_at", now_s)
            pulse.audit.setdefault("first_r1_price", truth.get("high"))
        # The trained label starts after the sealed cross anchor.  Touches inside
        # the forming/anchor candle are execution/no-chase truth, not future-bar
        # R50/R100 model outcomes.  Keep them in explicit cross-bar audit fields.
        on_cross_bar = pulse.state in {*FORMING_STATES, SEAL_AWAITING_FINAL_SCORE}
        if truth.get("hit_r50"):
            if on_cross_bar:
                pulse.audit.setdefault("cross_bar_hit_r50", True)
                pulse.audit.setdefault("target_consumed_in_cross_bar", True)
                pulse.audit.setdefault("target_consumed_at", now_s)
            else:
                pulse.audit.setdefault("hit_r50", True)
                pulse.audit.setdefault("first_r50_hit_at", now_s)
                pulse.audit.setdefault("first_r50_price", truth.get("high"))
        if truth.get("hit_r100"):
            if on_cross_bar:
                pulse.audit.setdefault("cross_bar_hit_r100", True)
            else:
                pulse.audit.setdefault("hit_r100", True)
                pulse.audit.setdefault("first_r100_hit_at", now_s)
                pulse.audit.setdefault("first_r100_price", truth.get("high"))
        pulse.audit["level_truth_last"] = truth
        return truth

    def _finalize_pending_for_new_bar(self, pulse: _Pulse, now_s: str, *, current_bar_time: str = "") -> Dict[str, Any]:
        if pulse.state not in FORMING_STATES:
            return self._row(pulse, event_type="snapshot")
        if bool((pulse.audit or {}).get("hit_r50") or (pulse.audit or {}).get("cross_bar_hit_r50")):
            return self._apply_atomic_state(
                pulse, TARGET_CONSUMED_BEFORE_OFFICIAL, now_s,
                reason="r50_reached_before_official_bar_change",
                event_type="target_consumed_before_official",
                extra={"current_bar_time": current_bar_time},
            )
        # R163: a new bar only closes the forming phase.  It must not guess the
        # sealed probability band or arm R1 before the exact sealed score exists.
        pulse.audit["seal_final_score_pending"] = True
        pulse.audit["pre_seal_state"] = pulse.state
        return self._apply_atomic_state(
            pulse, SEAL_AWAITING_FINAL_SCORE, now_s,
            reason="next_bar_arrived_exact_sealed_score_required",
            event_type="seal_awaiting_final_score",
            extra={"current_bar_time": current_bar_time},
        )

    def sweep_pending(
        self,
        *,
        market_key: Optional[str] = None,
        current_bar_time: Any = None,
        session_close: bool = False,
        swept_at: Any = None,
    ) -> List[Dict[str, Any]]:
        """R2 global finalizer: no LIVE_PENDING_SEAL survives bar change/session close."""
        now_s = _iso(swept_at)
        current_ts = _parse_bar_time(current_bar_time, market_key=market_key or "")
        rows: List[Dict[str, Any]] = []
        with self._lock:
            for pulse in list(self._by_id.values()):
                if market_key is not None and str(pulse.market_key or "") != str(market_key or ""):
                    continue
                if pulse.state not in (PERSISTED_ACTIVE_STATES if session_close else {*FORMING_STATES, SEAL_AWAITING_FINAL_SCORE}):
                    continue
                should_finalize = bool(session_close)
                if not should_finalize and current_ts is not None:
                    old_ts = _parse_bar_time(pulse.bar_time, market_key=pulse.market_key)
                    should_finalize = bool(old_ts is not None and current_ts > old_ts)
                if not should_finalize:
                    continue
                if session_close:
                    rows.append(self._apply_atomic_state(
                        pulse, EXPIRED_SESSION_CLOSE, now_s,
                        reason="session_close_pending_finalizer",
                        event_type="expired_session_close",
                    ))
                else:
                    if pulse.state == SEAL_AWAITING_FINAL_SCORE:
                        # Never invent an R1-watch state when the exact sealed score
                        # is unavailable.  Keep the episode retryable and let the
                        # probability chain record the explicit failure instead.
                        pulse.audit["seal_final_score_pending"] = True
                        pulse.audit["last_seal_retry_sweep_at"] = now_s
                        pulse.last_seen_at = now_s
                        continue
                    rows.append(self._finalize_pending_for_new_bar(
                        pulse, now_s, current_bar_time=str(current_bar_time or "")
                    ))
            self._prune_resolved()
        return rows

    def _open_forming_episode(
        self, *, obs: Dict[str, Any], prob: Dict[str, Any], paac_link: Dict[str, Any],
        paac_row: Optional[Mapping[str, Any]], market_key: str, symbol: str, bar_time: str,
        source_observation_id: str, price: float, birth_price: float, p50: float, p100: float,
        positive_cross_now: bool, raw_cross_known: bool, threshold: float, now_s: str,
        probability_scope: str, exact_probability: bool, key: Tuple[str, str],
    ) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []; first_cross_at = str(paac_link.get("first_cross_at") or now_s)
        first_cross_price = _finite(paac_link.get("first_cross_price"), birth_price)
        paac_bar = str(paac_link.get("signal_bar_time") or "")
        episode_bar = paac_bar or bar_time
        exact_p50 = p50 if exact_probability else float("nan")
        exact_p100 = p100 if exact_probability else float("nan")
        display_now = bool(exact_probability and _should_display_live(p50, market_key))
        arm_live_r1 = bool(exact_probability and _should_arm_r1_at_birth(p50, market_key))
        state_now = LIVE_PENDING_SEAL if display_now else LIVE_WAITING_R1 if arm_live_r1 else LIVE_INTERNAL_PENDING_SEAL
        levels = {
            k: prob.get(k) for k in (
                "anchor_price", "r1_price", "r50_price", "r100_price", "stop_price",
                "probability_kind", "model_version", "market_snapshot_symbols",
                "market_target_bar_time", "market_target_bar_symbols",
                "source_scan_complete", "coverage_scope", "probability_scope",
                "probability_asof", "probability_bar_time",
                "probability_source_observation_id",
            ) if exact_probability and prob.get(k) not in (None, "")
        }
        current = _Pulse(
            pulse_id=self._pulse_id(market_key, symbol, episode_bar),
            market_key=market_key, symbol=symbol, name=str(obs.get("name") or symbol),
            bar_time=episode_bar, threshold_pct=threshold,
            appearance_at=now_s if display_now else "",
            appearance_price=first_cross_price if display_now else float("nan"),
            first_cross_at=first_cross_at, first_cross_price=first_cross_price,
            p50_at_first_cross=exact_p50, p100_at_first_cross=exact_p100,
            qualified_at=now_s if display_now else "",
            qualification_price=first_cross_price if display_now else float("nan"),
            p50_at_qualification=exact_p50 if display_now else float("nan"),
            p100_at_qualification=exact_p100 if display_now else float("nan"),
            p50_live=exact_p50, p100_live=exact_p100, current_price=price,
            p50_live_latest=p50, p100_live_latest=p100,
            source_mtime_ns=int(obs.get("source_mtime_ns") or 0),
            state=state_now,
            probability_scope_first_cross=probability_scope if exact_probability else "UNAVAILABLE",
            probability_scope_live=probability_scope,
            probability_asof_first_cross=str(prob.get("probability_asof") or now_s) if exact_probability else "",
            probability_asof_live=str(prob.get("probability_asof") or now_s),
            last_seen_at=now_s, levels=dict(levels), live_levels=dict(levels),
            live_sniper_birth_proven=bool(display_now),
            live_sniper_born_at=now_s if display_now else "",
            live_sniper_source_observation_id=source_observation_id if exact_probability else "",
            audit={
                "rsiscaled": obs.get("rsiscaled"), "var3": obs.get("var3"),
                "pulse_gap": obs.get("pulse_gap"), "previous_gap": obs.get("previous_gap"),
                "probability_kind": prob.get("probability_kind"),
                "probability_scope": probability_scope,
                "probability_exact_at_cross": exact_probability,
                "birth_source_observation_id": source_observation_id,
                "birth_price_frozen": first_cross_price,
                "birth_probability_frozen": bool(exact_probability),
                "r1_watch_armed": bool(arm_live_r1),
                "r1_watch_mode": "LIVE_BIRTH_R1" if arm_live_r1 else "",
                "r1_watch_source_observation_id": source_observation_id if arm_live_r1 else "",
                "qualified_at_creation": display_now,
                "display_threshold_ever_reached": bool(display_now),
                "display_threshold_first_reached_at": now_s if display_now else "",
                "display_threshold_first_reached_scope": probability_scope if display_now else "",
                "display_threshold_pct": threshold,
                "qualification_state_now": "ACTIVE" if raw_cross_known else "INACTIVE",
                "model_available_now": bool(prob.get("available", math.isfinite(p50))),
                "positive_cross_now": bool(positive_cross_now),
                "source_observation_id": source_observation_id,
                "source_file": obs.get("source_file"),
                "source_fields": obs.get("source_fields") or obs.get("fields") or 8,
                "live_sniper_cross_detected_at": prob.get("live_sniper_cross_detected_at") or obs.get("observed_at"),
                "live_sniper_probability_started_at": prob.get("live_sniper_probability_started_at"),
                "live_sniper_probability_finished_at": prob.get("live_sniper_probability_finished_at"),
                "live_sniper_probability_compute_ms": prob.get("live_sniper_probability_compute_ms"),
                "live_sniper_cross_to_probability_ms": prob.get("live_sniper_cross_to_probability_ms"),
                "live_sniper_score_kind": prob.get("live_sniper_score_kind"),
                "raw_cross_seen": True,
                "model_horizon_bars": HORIZON_BARS,
                **_bar_window_contract(obs),
            },
            linked_paac_episode_id=str(paac_link.get("episode_id") or ""),
            linked_paac_snapshot=dict(paac_link),
        )
        initial_level_truth = self._apply_level_truth(current, obs, now_s)
        current.audit["execution_layer_result"] = _execution_eval_for_pulse(current, obs, prob, paac_row=paac_link)
        terminal_proposal: Optional[_Pulse] = None
        if _is_late_no_chase(paac_link):
            terminal_proposal = _clone_pulse(current)
            terminal_proposal.audit["late_no_chase_reason_code"] = str((paac_row or {}).get("late_no_chase_reason_code") or (paac_row or {}).get("pulse_acceptance_reason") or "PAAC_LIVE_LATE_NO_CHASE")
            terminal_proposal.state = LIVE_LATE_NO_CHASE
            terminal_proposal.sealed_at = now_s
            terminal_proposal.audit["seal_final_score_pending"] = False
            event_type = "live_late_no_chase"
        elif _is_live_failed(paac_link):
            terminal_proposal = _clone_pulse(current)
            terminal_proposal.state = SEALED_REJECTED
            terminal_proposal.sealed_at = now_s
            terminal_proposal.audit["seal_failure_reason"] = "paac_live_failed"
            terminal_proposal.audit["seal_final_score_pending"] = False
            event_type = "paac_live_failed_rejected"
        elif bool(initial_level_truth.get("hit_r50") or current.audit.get("cross_bar_hit_r50")):
            terminal_proposal = _clone_pulse(current)
            terminal_proposal.state = TARGET_CONSUMED_BEFORE_ENTRY
            terminal_proposal.sealed_at = now_s
            terminal_proposal.audit["seal_final_score_pending"] = False
            event_type = "target_consumed_before_entry"
        else:
            event_type = LIVE_P30_BORN if display_now else "live_r1_watch_armed" if arm_live_r1 else "raw_cross_internal"
        self._by_symbol[key] = current
        self._by_id[current.pulse_id] = current
        self._prune_resolved()
        self._journal(current, "raw_cross_detected", {"probability_scope": probability_scope, "exact_probability": exact_probability})
        if terminal_proposal is not None:
            _commit_terminal_proposal(self, current, terminal_proposal, event_type, {"reason": event_type})
        _append_episode_event("RAW_CROSS_DETECTED", self._row(current, event_type="raw_cross_detected"), suppress_duplicate=True)
        if math.isfinite(p50):
            _append_episode_event("PROBABILITY_AT_CROSS" if exact_probability else "PROBABILITY_AVAILABLE_AFTER_CROSS", self._row(current, event_type=event_type), suppress_duplicate=True)
        if display_now:
            self._journal(current, event_type)
            row = self._row(current, event_type=event_type)
            _append_episode_event(LIVE_P30_BORN, row, extra={"event_origin": ORIGIN_LIVE_SOURCE_OBSERVATION}, suppress_duplicate=True)
            out.append(row)
        return out


    def observe_forming(
        self,
        observation: Mapping[str, Any],
        probability: Mapping[str, Any],
        *,
        observed_at: Any = None,
        paac_row: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Observe a forming M30 bar without rewriting birth truth.

        The immutable birth ledger belongs to the exact source observation that
        carried the RSIScaled/VAR3 cross.  Model completion may arrive later, but
        it may populate birth fields only when its source_observation_id is the
        same.  Later file updates can refresh current price and audit probability;
        they never move appearance price or overwrite P(BIRTH).
        """
        obs = dict(observation or {})
        prob = dict(probability or {})
        paac_link = _linked_paac_snapshot(paac_row)
        market_key = str(obs.get("market_key") or "")
        symbol = str(obs.get("symbol") or "").strip().upper()
        bar_time = str(obs.get("bar_datetime") or "")
        source_observation_id = str(obs.get("source_observation_id") or "")
        birth_price, price = _birth_and_current_price(obs)
        p50 = _finite(prob.get("p50_pct"))
        p100 = _finite(prob.get("p100_pct"))
        gap = _finite(obs.get("pulse_gap"))
        prev_gap = _finite(obs.get("previous_gap"))
        positive_cross_now = bool(obs.get("positive_cross_now")) or (
            math.isfinite(gap) and math.isfinite(prev_gap) and prev_gap < 0.0 <= gap
        )
        paac_bar = str(paac_link.get("signal_bar_time") or "")
        paac_state = _paac_state_text(paac_link)
        linked_raw_cross = bool(paac_link and paac_bar == bar_time and paac_state.startswith("LIVE_"))
        raw_cross_known = bool(positive_cross_now or linked_raw_cross)
        explicit_cross_state = bool(
            "positive_cross_now" in obs or "technical_signal_live" in obs
            or math.isfinite(gap)
        )
        cross_active_now = bool(
            positive_cross_now
            or _truthy(obs.get("technical_signal_live"))
            or (math.isfinite(gap) and gap >= 0.0)
        )
        if not market_key or not symbol or not bar_time or not math.isfinite(price) or price <= 0:
            return []

        threshold = threshold_pct_for_market(market_key)
        now_s = _iso(observed_at or obs.get("observed_at"))
        probability_scope = _infer_probability_scope(prob, obs)
        exact_probability = bool(_exact_probability_for_observation(prob, obs))
        key = self._key(market_key, symbol)
        out: List[Dict[str, Any]] = []

        with self._lock:
            # A2 live-session hardening: an already-terminal *same episode* is
            # immutable.  Later observations from the same forming bar may update
            # audit facts elsewhere, but they may never reopen the episode at a
            # lower truth rank.  This closes the live-session 4338/1050/6020/4348
            # terminal-reopen pattern observed on 2026-08-09.
            episode_bar = paac_bar or bar_time
            episode_id = self._pulse_id(market_key, symbol, episode_bar)
            pending_terminal_intent = dict(self._pending_terminal_intent_by_alias.get(episode_id) or {})
            if pending_terminal_intent:
                # The prior process proposed this exact terminal transition and
                # durably recorded a retry debt, but Terminal authority is not
                # yet available.  Fail closed: do not create a second Episode.
                return out
            durable_terminal = dict(self._durable_terminal_by_alias.get(episode_id) or {})
            if not durable_terminal:
                # Handles canonical textual aliases (.000 vs legacy form) without
                # adding source-path I/O: this lookup is only reached on a raw cross.
                durable_terminal = dict(_terminal_truth_for_episode(episode_id, path=self._terminal_truth_path) or {})
                if durable_terminal:
                    for alias in tuple(durable_terminal.get("episode_aliases") or (episode_id,)):
                        alias_text = str(alias or "").strip()
                        if alias_text:
                            self._durable_terminal_by_alias[alias_text] = durable_terminal
            terminal_episode = self._by_id.get(episode_id)
            terminal_in_memory = bool(
                terminal_episode is not None
                and terminal_episode.state in TERMINAL_STATES
                and not bool((terminal_episode.audit or {}).get("seal_final_score_pending"))
            )
            if durable_terminal or terminal_in_memory:
                if terminal_episode is not None:
                    terminal_episode.audit["terminal_reopen_suppressed_count"] = int(
                        (terminal_episode.audit or {}).get("terminal_reopen_suppressed_count") or 0
                    ) + 1
                    terminal_episode.audit["terminal_reopen_last_seen_at"] = now_s
                    terminal_episode.audit["terminal_reopen_last_source_observation_id"] = source_observation_id
                    terminal_episode.audit["terminal_reopen_suppressed_by_durable_authority"] = bool(durable_terminal)
                return out

            current = self._by_symbol.get(key)
            if current is not None and current.state in FORMING_STATES and current.bar_time != bar_time:
                transition = self._finalize_pending_for_new_bar(current, now_s, current_bar_time=bar_time)
                if not bool(transition.get("internal_only")):
                    out.append(transition)
                self._by_id[current.pulse_id] = current
                if self._by_symbol.get(key) is current:
                    self._by_symbol.pop(key, None)
                self._expire_excess_pending_for_symbol(key)

            current = self._by_symbol.get(key)
            if current is None or current.bar_time != bar_time or current.state not in FORMING_STATES:
                if not raw_cross_known:
                    return out
                return out + self._open_forming_episode(
                    obs=obs, prob=prob, paac_link=paac_link, paac_row=paac_row,
                    market_key=market_key, symbol=symbol, bar_time=bar_time,
                    source_observation_id=source_observation_id, price=price, birth_price=birth_price, p50=p50, p100=p100,
                    positive_cross_now=positive_cross_now, raw_cross_known=raw_cross_known,
                    threshold=threshold, now_s=now_s, probability_scope=probability_scope,
                    exact_probability=exact_probability, key=key,
                )

            # Same episode/bar: update current-market truth only. Birth truth is
            # filled once by the exact original observation and then immutable.
            current.p50_live_latest = p50 if math.isfinite(p50) else current.p50_live_latest
            current.p100_live_latest = p100 if math.isfinite(p100) else current.p100_live_latest
            current.probability_scope_live = probability_scope
            current.probability_asof_live = str(prob.get("probability_asof") or now_s)
            current.current_price, current.last_seen_at = price, now_s
            current.source_mtime_ns = max(current.source_mtime_ns, int(obs.get("source_mtime_ns") or 0))
            current.audit["qualification_state_now"] = "ACTIVE" if raw_cross_known else "INACTIVE"
            current.audit["model_available_now"] = bool(prob.get("available", math.isfinite(p50)))
            current.audit["positive_cross_now"] = bool(positive_cross_now)
            current.audit["probability_scope"] = probability_scope
            current.audit["source_observation_id"] = source_observation_id
            _backfill_bar_window(current.audit, obs)
            if obs.get("source_file"):
                current.audit["source_file"] = obs.get("source_file")
            if obs.get("source_fields") or obs.get("fields"):
                current.audit["source_fields"] = obs.get("source_fields") or obs.get("fields")
            for _lat_key in (
                "live_sniper_cross_detected_at", "live_sniper_probability_started_at",
                "live_sniper_probability_finished_at", "live_sniper_probability_compute_ms",
                "live_sniper_cross_to_probability_ms", "live_sniper_score_kind",
            ):
                if prob.get(_lat_key) not in (None, ""):
                    current.audit[_lat_key] = prob.get(_lat_key)
            incoming_paac_id = str(paac_link.get("episode_id") or "")
            if incoming_paac_id and not current.linked_paac_episode_id:
                current.linked_paac_episode_id = incoming_paac_id
                current.linked_paac_snapshot = dict(paac_link)
            elif incoming_paac_id and incoming_paac_id == current.linked_paac_episode_id:
                current.linked_paac_snapshot.update(dict(paac_link))

            birth_source_id = str(current.audit.get("birth_source_observation_id") or "")
            same_birth_observation = bool(exact_probability and source_observation_id and source_observation_id == birth_source_id)
            birth_score_missing = not math.isfinite(current.p50_at_first_cross)
            if same_birth_observation and birth_score_missing and math.isfinite(p50):
                current.p50_at_first_cross = p50
                current.p100_at_first_cross = p100
                current.p50_live = p50
                current.p100_live = p100
                current.probability_scope_first_cross = probability_scope
                current.probability_asof_first_cross = str(prob.get("probability_asof") or now_s)
                current.live_sniper_source_observation_id = source_observation_id
                current.levels = {
                    k: prob.get(k) for k in (
                        "anchor_price", "r1_price", "r50_price", "r100_price", "stop_price",
                        "probability_kind", "model_version", "probability_scope", "probability_asof",
                        "probability_bar_time", "probability_source_observation_id",
                    ) if prob.get(k) not in (None, "")
                }
                current.live_levels = dict(current.levels)
                current.audit["birth_probability_frozen"] = True
                current.audit["probability_exact_at_cross"] = True
                if _should_display_live(p50, market_key):
                    current.state = LIVE_PENDING_SEAL
                    current.appearance_at = now_s
                    current.appearance_price = current.first_cross_price
                    current.qualified_at = now_s
                    current.qualification_price = current.first_cross_price
                    current.p50_at_qualification = p50
                    current.p100_at_qualification = p100
                    current.live_sniper_birth_proven = True
                    current.live_sniper_born_at = now_s
                    current.audit.update({
                        "display_threshold_ever_reached": True,
                        "display_threshold_first_reached_at": current.audit.get("display_threshold_first_reached_at") or now_s,
                        "display_threshold_first_reached_scope": current.audit.get("display_threshold_first_reached_scope") or probability_scope,
                        "display_threshold_pct": current.threshold_pct,
                    })
                    event_type = LIVE_P30_BORN
                    self._journal(current, event_type, {"delayed_same_observation_score": True})
                    row = self._row(current, event_type=event_type)
                    _append_episode_event(LIVE_P30_BORN, row, extra={"event_origin": ORIGIN_LIVE_SOURCE_OBSERVATION}, suppress_duplicate=True)
                    out.append(row)
                elif _should_arm_r1_at_birth(p50, market_key):
                    current.state = LIVE_WAITING_R1
                    current.audit["r1_watch_armed"] = True
                    current.audit["r1_watch_mode"] = "LIVE_BIRTH_R1"
                    current.audit["r1_watch_armed_at"] = now_s
                    current.audit["r1_watch_source_observation_id"] = source_observation_id
                    self._journal(current, "live_r1_watch_armed", {"delayed_same_observation_score": True})

            level_truth = self._apply_level_truth(current, obs, now_s)
            current.audit["execution_layer_result"] = _execution_eval_for_pulse(current, obs, prob, paac_row=current.linked_paac_snapshot or paac_link)

            # H12H14 attention contract: the immutable first-cross probability
            # remains frozen for audit/model truth, but the *display* threshold is
            # evaluated on each exact same-bar source observation.  A later exact
            # score may promote a background episode once the market threshold is
            # actually reached; this does not rewrite p50_at_first_cross and does
            # not pretend the episode was P30 at birth.
            if exact_probability and not same_birth_observation and math.isfinite(p50):
                current.audit.setdefault("later_observation_probability_seen_at", now_s)
                current.audit["later_observation_p50_latest"] = p50
                current.audit["later_observation_p100_latest"] = p100
                if p50 >= threshold and not current.appearance_at:
                    current.state = LIVE_PENDING_SEAL
                    current.appearance_at = now_s
                    current.appearance_price = price
                    current.qualified_at = current.qualified_at or now_s
                    current.qualification_price = price
                    current.p50_at_qualification = p50
                    current.p100_at_qualification = p100
                    current.audit.update({
                        "display_threshold_ever_reached": True,
                        "display_threshold_first_reached_at": now_s,
                        "display_threshold_first_reached_scope": probability_scope,
                        "display_threshold_pct": threshold,
                        "threshold_promotion_from_later_observation": True,
                        "threshold_promotion_source_observation_id": source_observation_id,
                    })
                    self._journal(current, LIVE_P30_UPDATED, {
                        "threshold_attention_promotion": True,
                        "first_cross_probability_immutable": True,
                    })
                    row = self._row(current, event_type=LIVE_P30_UPDATED)
                    _append_episode_event(
                        LIVE_P30_UPDATED, row,
                        extra={
                            "event_origin": ORIGIN_LIVE_SOURCE_OBSERVATION,
                            "threshold_attention_promotion": True,
                        },
                        suppress_duplicate=True,
                    )
                    out.append(row)

            if _is_late_no_chase(paac_link):
                current.audit["late_no_chase_reason_code"] = str((paac_row or {}).get("late_no_chase_reason_code") or (paac_row or {}).get("pulse_acceptance_reason") or "PAAC_LIVE_LATE_NO_CHASE")
                row = self._apply_atomic_state(current, LIVE_LATE_NO_CHASE, now_s, reason="paac_live_late_no_chase", event_type="live_late_no_chase")
                _append_episode_event("NO_CHASE", row)
                return out + ([row] if current.appearance_at else [])
            if _is_live_failed(paac_link):
                current.audit["seal_failure_reason"] = "paac_live_failed"
                row = self._apply_atomic_state(current, SEALED_REJECTED, now_s, reason="paac_live_failed", event_type="paac_live_failed_rejected")
                _append_episode_event("LIVE_FAILED", row)
                return out + ([row] if current.appearance_at else [])
            if bool(level_truth.get("hit_r50")) and current.state in {LIVE_PENDING_SEAL, LIVE_WEAKENING}:
                row = self._apply_atomic_state(current, TARGET_CONSUMED_BEFORE_ENTRY, now_s, reason="r50_reached_on_source_observation_before_entry", event_type="target_consumed_before_entry")
                _append_episode_event("TARGET_CONSUMED_BEFORE_ENTRY", row)
                return out + [row]

            if current.state == LIVE_WEAKENING and cross_active_now:
                recovered_state = str(current.audit.pop("weakening_return_state", "") or LIVE_PENDING_SEAL)
                if recovered_state not in {LIVE_PENDING_SEAL, LIVE_WAITING_R1, LIVE_INTERNAL_PENDING_SEAL}:
                    recovered_state = LIVE_PENDING_SEAL
                current.state = recovered_state
                current.audit["weakening_recovered_at"] = now_s
                current.audit["qualification_state_now"] = "ACTIVE"
                self._journal(current, "LIVE_CROSS_RECOVERED", {"recovered_state": recovered_state})
                if current.appearance_at:
                    row = self._row(current, event_type="LIVE_CROSS_RECOVERED")
                    _append_episode_event("LIVE_CROSS_RECOVERED", row, suppress_duplicate=True)
                    out.append(row)
            elif (
                current.state in {LIVE_PENDING_SEAL, LIVE_WAITING_R1, LIVE_INTERNAL_PENDING_SEAL}
                and current.appearance_at and explicit_cross_state and not cross_active_now
            ):
                current.audit["weakening_return_state"] = current.state
                current.audit["weakening_started_at"] = now_s
                current.audit["qualification_state_now"] = "INACTIVE"
                current.state = LIVE_WEAKENING
                self._journal(current, LIVE_WEAKENING, {"positive_cross_now": False})
                row = self._row(current, event_type=LIVE_WEAKENING)
                _append_episode_event(LIVE_WEAKENING, row, suppress_duplicate=True)
                return out + [row]

            if current.state in {LIVE_INTERNAL_PENDING_SEAL, LIVE_WAITING_R1}:
                return out
            price_bucket = int(round((current.current_price / max(current.appearance_price, 1e-12)) * 1000.0))
            signature = (round(current.p50_live, 1), price_bucket, bool(raw_cross_known), _paac_state_text(paac_link), bool(current.audit.get("hit_r50") or current.audit.get("cross_bar_hit_r50")))
            if signature != current.last_emit_signature:
                current.last_emit_signature = signature
                out.append(self._row(current, event_type=LIVE_P30_UPDATED))
            return out

    def _terminal_cross_failure_row(self, pulse: _Pulse, final_prob: Dict[str, Any], now_s: str) -> List[Dict[str, Any]]:
        proposal = _clone_pulse(pulse)
        proposal.p50_final = proposal.p100_final = float("nan")
        proposal.probability_scope_sealed = PROB_SCOPE_SEALED_CROSS_BAR
        proposal.probability_asof_sealed = str(final_prob.get("probability_asof") or now_s)
        reason_code = str(
            final_prob.get("failure_reason_code") or final_prob.get("reason_code")
            or "CROSS_NOT_STABLE_AT_SEAL"
        ).upper()
        proposal.audit = dict(proposal.audit or {})
        proposal.audit.update({
            "seal_final_score_pending": False,
            "seal_failure_reason": "intrabar_cross_failed_at_seal",
            "seal_failure_reason_code": reason_code,
            "seal_terminal_probability_status": str(final_prob.get("status") or final_prob.get("pipeline_status") or ""),
            "seal_committed": True,
            "seal_committed_fingerprint": repr(_seal_fingerprint({}, final_prob)),
            "seal_committed_at": proposal.sealed_at or now_s,
        })
        proposal.state = INTRABAR_CROSS_FAILED_AT_SEAL
        event_type = "INTRABAR_CROSS_FAILED_AT_SEAL"
        _commit_terminal_proposal(
            self, pulse, proposal, event_type,
            {"final_cross": False, "terminal_no_retry": True, "failure_reason_code": reason_code},
        )
        row = self._row(pulse, event_type=event_type)
        _append_episode_event(
            event_type, row, extra={"event_origin": ORIGIN_SEALED_CROSS_BAR}, suppress_duplicate=True,
        )
        self._prune_resolved()
        return [row] if pulse.appearance_at else []



    def _finish_sealed_probability(
        self, pulse: _Pulse, *, final_obs: Dict[str, Any], final_prob: Dict[str, Any],
        market_key: str, bar_time: str, p50: float, p100: float, now_s: str,
        prior_count: int, required: int,
    ) -> List[Dict[str, Any]]:
        proposal = _clone_pulse(pulse)
        proposal.p50_final, proposal.p100_final = p50, p100
        proposal.probability_scope_sealed = PROB_SCOPE_SEALED_CROSS_BAR
        proposal.probability_asof_sealed = str(final_prob.get("probability_asof") or now_s)
        proposal.sealed_model_anchor_at = now_s
        proposal.sealed_model_anchor_price = _finite(final_prob.get("anchor_price"))
        proposal.audit = dict(proposal.audit or {})
        proposal.audit["seal_final_score_pending"] = False
        proposal.audit["probability_scope_sealed"] = PROB_SCOPE_SEALED_CROSS_BAR
        proposal.sealed_levels = _sealed_probability_levels(
            final_prob, final_obs, bar_time, proposal.probability_asof_sealed,
        )
        effective_manifest = _truth_effective_financial({
            "episode_id": proposal.pulse_id, "pulse_episode_id": proposal.pulse_id,
            "signal_bar_time": proposal.bar_time,
            "live_pulse_seal_state": "SEALED_TRUTH_PENDING_CLASSIFICATION",
            "p50_final": proposal.p50_final, "p100_final": proposal.p100_final,
            "sealed_gann_anchor_price": proposal.sealed_levels.get("anchor_price"),
            "sealed_gann_r1_price": proposal.sealed_levels.get("r1_price"),
            "sealed_gann_r50_price": proposal.sealed_levels.get("r50_price"),
            "sealed_gann_r100_price": proposal.sealed_levels.get("r100_price"),
            "sealed_gann_stop_price": proposal.sealed_levels.get("stop_price"),
        })
        proposal.audit["sealed_truth_complete"] = bool(effective_manifest.get("sealed_truth_complete"))
        proposal.audit["sealed_truth_missing_fields"] = list(effective_manifest.get("sealed_truth_missing_fields") or [])
        proposal.audit["effective_truth_scope"] = str(effective_manifest.get("effective_truth_scope") or "")
        proposal.audit["sealed_truth_id"] = str(effective_manifest.get("sealed_truth_id") or "")
        proposal.audit["effective_truth_id"] = str(effective_manifest.get("effective_truth_id") or "")
        proposal.audit["effective_truth_contract_version"] = str(effective_manifest.get("effective_truth_contract_version") or "")

        if not bool(effective_manifest.get("sealed_truth_complete")):
            # Exact probability exists, but its financial geometry is incomplete.
            # Do not mix birth levels into a SEALED row and do not invent a
            # terminal outcome.  Keep the same episode retryable immediately.
            proposal.state = SEAL_AWAITING_FINAL_SCORE
            proposal.last_seen_at = now_s
            proposal.audit["seal_final_score_pending"] = True
            proposal.audit["seal_failure_reason"] = "sealed_financial_geometry_incomplete"
            proposal.audit["seal_failure_reason_code"] = "SEALED_FINANCIAL_GEOMETRY_INCOMPLETE"
            _replace_pulse_from_proposal(pulse, proposal)
            self._journal(
                pulse, "SEALED_FINANCIAL_GEOMETRY_INCOMPLETE",
                {"missing_fields": list(proposal.audit.get("sealed_truth_missing_fields") or [])},
            )
            row = self._row(pulse, event_type="SEALED_FINANCIAL_GEOMETRY_INCOMPLETE")
            _append_episode_event(
                "SEALED_FINANCIAL_GEOMETRY_INCOMPLETE", row,
                extra={"event_origin": ORIGIN_SEALED_CROSS_BAR}, suppress_duplicate=True,
            )
            return [row] if pulse.appearance_at else []

        proposal.audit["execution_layer_result"] = _execution_eval_for_pulse(
            proposal,
            {**final_obs, "close": proposal.signal_bar_close, "current_price": proposal.signal_bar_close},
            final_prob, paac_row=proposal.linked_paac_snapshot,
        )
        sealed_r50 = _finite(proposal.sealed_levels.get("r50_price"))
        target_consumed = bool(math.isfinite(sealed_r50) and proposal.signal_bar_high >= sealed_r50)
        threshold = threshold_pct_for_market(market_key)
        was_visible = bool(proposal.appearance_at)
        # H12H14 visibility contract: model threshold is the sole promotion gate.
        # A sealed score may be the first moment the episode reaches the market
        # threshold; latch that fact permanently without pretending it was a live
        # birth.  No-Chase/terminal policy never hides an episode that the model
        # has already promoted.
        if p50 >= threshold and not was_visible:
            _promotion_price = _finite(final_obs.get("current_price"))
            if not math.isfinite(_promotion_price):
                _promotion_price = _finite(final_obs.get("close"))
            if not math.isfinite(_promotion_price):
                _promotion_price = proposal.signal_bar_close
            proposal.appearance_at = now_s
            proposal.appearance_price = _promotion_price if math.isfinite(_promotion_price) else proposal.appearance_price
            proposal.qualified_at = proposal.qualified_at or now_s
            proposal.qualification_price = (
                _promotion_price if math.isfinite(_promotion_price) else proposal.qualification_price
            )
            proposal.p50_at_qualification = p50
            proposal.p100_at_qualification = p100
            proposal.audit.update({
                "display_threshold_ever_reached": True,
                "display_threshold_first_reached_at": now_s,
                "display_threshold_first_reached_scope": PROB_SCOPE_SEALED_CROSS_BAR,
                "display_threshold_pct": threshold,
            })
            was_visible = True
        if target_consumed:
            proposal.state, event_type = TARGET_CONSUMED_BEFORE_OFFICIAL, "target_consumed_before_official"
        elif p50 >= threshold:
            proposal.state, event_type = SEALED_P30_CONFIRMED, SNIPER_SEALED_P30_CONFIRMED
        elif _should_arm_r1_after_seal(p50, market_key):
            proposal.state = SEALED_WAITING_R1
            proposal.audit.update({
                "r1_watch_armed": True, "r1_watch_mode": "SEALED_R1", "r1_watch_armed_at": now_s,
            })
            event_type = P30_DOWNGRADED_TO_R1_WATCH if was_visible else SEALED_R1_WATCH_ARMED
        else:
            proposal.state, event_type = SEALED_REJECTED, "SEALED_REJECTED_LOW_PROBABILITY"
            proposal.audit.update({
                "r1_watch_armed": False, "seal_failure_reason": "sealed_probability_below_20",
            })
        proposal.audit["seal_committed"] = True
        proposal.audit["seal_committed_fingerprint"] = str(proposal.audit.get("seal_candidate_fingerprint") or "")
        proposal.audit["seal_committed_at"] = proposal.sealed_at or now_s
        journal_extra = {
            "final_cross": True, "stable_count": max(1, prior_count), "stable_required": required,
            "birth_probability_immutable": True, "sealed_probability_independent": True,
        }
        if proposal.state in TERMINAL_STATES:
            _commit_terminal_proposal(self, pulse, proposal, event_type, journal_extra)
        else:
            _replace_pulse_from_proposal(pulse, proposal)
            self._journal(pulse, event_type, journal_extra)
        row = self._row(pulse, event_type=event_type)
        _append_episode_event(
            str(event_type).upper(), row,
            extra={"event_origin": ORIGIN_SEALED_CROSS_BAR}, suppress_duplicate=True,
        )
        self._prune_resolved()
        return [row]



    def seal_previous(
        self,
        *,
        market_key: str,
        symbol: str,
        bar_time: str,
        final_observation: Mapping[str, Any],
        final_probability: Mapping[str, Any],
        sealed_at: Any = None,
    ) -> List[Dict[str, Any]]:
        """Seal one exact M30 cross bar exactly once.

        File stability, not a second market tick, authorizes the seal.  The sealed
        model ledger is independent from immutable birth price/probability/levels.
        A cross that disappeared at close is a terminal trading result, not a model
        timeout and never re-enters the pending queue.
        """
        key = self._key(market_key, symbol)
        final_obs = dict(final_observation or {})
        now_s = _iso(sealed_at)
        final_prob = _stamp_sealed_probability(
            dict(final_probability or {}), bar_time=bar_time, calculated_at=now_s,
            source_observation_id=final_obs.get("source_observation_id"),
        )
        with self._lock:
            pulse_id = self._pulse_id(market_key, symbol, str(bar_time or ""))
            pulse = self._by_id.get(pulse_id)
            if pulse is None:
                candidate = self._by_symbol.get(key)
                pulse = candidate if candidate is not None and candidate.bar_time == str(bar_time or "") else None
            if pulse is None or pulse.bar_time != str(bar_time or ""):
                return []
            if pulse.state in TERMINAL_STATES and not bool(pulse.audit.get("seal_final_score_pending")):
                return []

            # A caller may pass the exact old bar directly. Never use nearest/current
            # bar fallback. The source reader marks snapshots stable only when file
            # stat is unchanged across the read.
            if not bool(final_obs.get("sealed_close_verified")) and _truth_wall_time_key(final_obs.get("bar_datetime") or final_obs.get("date") or final_obs.get("time")) == _truth_wall_time_key(bar_time):
                final_obs = _truth_stamp_exact_sealed_bar(
                    final_obs, final_obs, bar_time, sealed_at=sealed_at, source_kind="EXACT_SOURCE_BAR"
                )
            exact_close, close_reason = _truth_verified_sealed_close(final_obs, signal_bar_time=bar_time)
            if exact_close is None:
                pulse.audit["sealed_close_verified"] = False
                pulse.audit["sealed_close_reason_code"] = close_reason
                pulse.audit["sealed_close_source"] = ""
                pulse.audit["sealed_source_bar_time"] = final_obs.get("sealed_source_bar_time")
                pulse.audit["seal_final_score_pending"] = True
                pulse.audit["seal_failure_reason"] = "exact_sealed_source_close_unavailable"
                pulse.state = SEAL_AWAITING_FINAL_SCORE
                pulse.last_seen_at = now_s
                self._journal(pulse, "sealed_close_source_unavailable", {"reason_code": close_reason})
                row = self._row(pulse, event_type="SEALED_CLOSE_SOURCE_UNAVAILABLE")
                _append_episode_event("SEALED_CLOSE_SOURCE_UNAVAILABLE", row, suppress_duplicate=True)
                return [row] if pulse.appearance_at else []

            # If the source reader explicitly reports instability, retry the file
            # read immediately upstream; do not wait for another market update.
            if final_obs.get("source_read_stable") is False:
                pulse.audit["seal_final_score_pending"] = True
                pulse.audit["seal_failure_reason"] = "source_changed_during_exact_bar_read"
                pulse.state = SEAL_AWAITING_FINAL_SCORE
                pulse.last_seen_at = now_s
                return []

            incoming_seal_fingerprint = repr(_seal_fingerprint(final_obs, final_prob))
            if bool(pulse.audit.get("seal_committed")):
                committed_fingerprint = str(pulse.audit.get("seal_committed_fingerprint") or "")
                if committed_fingerprint and committed_fingerprint != incoming_seal_fingerprint:
                    raise SealPayloadConflict(
                        f"SEAL_PAYLOAD_CONFLICT:episode={pulse.pulse_id}:bar={bar_time}"
                    )
                return []

            p50 = _finite(final_prob.get("p50_pct"))
            p100 = _finite(final_prob.get("p100_pct"))
            gap = _finite(final_obs.get("pulse_gap"))
            prev_gap = _finite(final_obs.get("previous_gap"))
            final_cross = bool(final_obs.get("positive_cross_now")) or (
                math.isfinite(gap) and math.isfinite(prev_gap) and prev_gap < 0.0 <= gap
            )
            failure_reason_code = str(final_prob.get("failure_reason_code") or final_prob.get("reason_code") or "").upper()
            terminal_cross_reasons = {"CROSS_NOT_STABLE_AT_SEAL", "CROSS_NOT_ACTIVE_AT_SCORE"}
            cross_failed = bool((not final_cross) or failure_reason_code in terminal_cross_reasons)

            fingerprint = _seal_fingerprint(final_obs, final_prob)
            required = seal_stable_observations_required()
            fp_text = repr(fingerprint)
            prior_fp = str(pulse.audit.get("seal_candidate_fingerprint") or "")
            if required <= 1:
                pulse.audit["seal_candidate_fingerprint"] = fp_text
            prior_count = int(pulse.audit.get("seal_candidate_count") or 0)
            if pulse.state in FORMING_STATES:
                pulse.state = SEAL_AWAITING_FINAL_SCORE
                pulse.audit["seal_final_score_pending"] = True
            if required > 1:
                if prior_fp != fp_text:
                    prior_count = 0
                prior_count += 1
                pulse.audit["seal_candidate_fingerprint"] = fp_text
                pulse.audit["seal_candidate_count"] = prior_count
                pulse.audit["seal_candidate_required"] = required
                if prior_count < required:
                    pulse.last_seen_at = now_s
                    return []

            pulse.signal_bar_close = float(exact_close)
            pulse.signal_bar_high = _best_high(final_obs, fallback_price=pulse.signal_bar_close)
            latest_source_price = _finite(final_obs.get("source_latest_close"))
            if math.isfinite(latest_source_price) and latest_source_price > 0:
                pulse.current_price = latest_source_price
            pulse.sealed_at = now_s
            pulse.last_seen_at = now_s
            pulse.audit.update(sealed_close_audit_fields(final_obs, bar_time=bar_time, verified_at=now_s))
            pulse.audit["model_horizon_bars"] = HORIZON_BARS

            # Preserve any live R1 proof before classifying the sealed model lane.
            try:
                from pulse_tick_tape import episode_truth as _tick_episode_truth
                tick_truth = dict(_tick_episode_truth(pulse.pulse_id) or {})
            except Exception:
                tick_truth = {}
            if tick_truth.get("activated_r1"):
                pulse.audit["live_r1_birth_proven"] = True
                pulse.audit["first_r1_crossed_at"] = tick_truth.get("first_r1_tick_ts")
                pulse.audit["first_r1_price"] = tick_truth.get("first_r1_price")

            if cross_failed:
                return self._terminal_cross_failure_row(pulse, final_prob, now_s)

            # A still-active cross with a technical model failure remains retryable.
            if not bool(final_prob.get("available", math.isfinite(p50))) or not math.isfinite(p50):
                pulse.audit["seal_final_score_pending"] = True
                pulse.audit["seal_failure_reason"] = str(final_prob.get("failure_reason_code") or final_prob.get("error") or "sealed_probability_unavailable")
                pulse.state = SEAL_AWAITING_FINAL_SCORE
                return [self._row(pulse, event_type="SEAL_AWAITING_FINAL_SCORE")] if pulse.appearance_at else []

            return self._finish_sealed_probability(
                pulse, final_obs=final_obs, final_prob=final_prob, market_key=market_key,
                bar_time=bar_time, p50=p50, p100=p100, now_s=now_s,
                prior_count=prior_count, required=required,
            )

    def _row(self, pulse: Optional[_Pulse], *, event_type: str) -> Dict[str, Any]:
        if pulse is None:
            return {}
        (
            state, internal_pending, live_waiting, live_pending, awaiting, sealed_p30,
            sealed_waiting, sealed_late_center, rejected, cross_failed_at_seal, no_chase, failed,
            expired, target_consumed, final_score_pending, seal_failure_reason, target_reached_before_official,
            execution_result, execution_flat, execution_passed, execution_shadow_passed, execution_authorized,
            execution_hard_blocked, execution_publishable, publishable, stage, status, p50_show, p100_show,
            entry_status, professional_type, visible_case, truth_scope, monitor_outcome,
        ) = _row_presentation(pulse)
        levels = dict(pulse.levels or {})
        live_levels = dict(pulse.live_levels or {})
        sealed_levels = dict(pulse.sealed_levels or {})
        probability_kind = str((sealed_levels if pulse.probability_scope_sealed else levels).get("probability_kind") or levels.get("probability_kind") or (pulse.audit or {}).get("probability_kind") or "")
        sealed_probability_available, probability_display_scope, probability_display_asof, probability_display_kind = _probability_display_truth(pulse, levels, sealed_levels, p50_show, probability_kind)
        episode_outcome, episode_outcome_ar = _TERMINAL_OUTCOME_PRESENTATION.get(state, (monitor_outcome or state or "UNKNOWN", visible_case or status or "غير محدد"))
        sniper_event_type = str(event_type or "")
        if live_pending and sniper_event_type not in {LIVE_P30_BORN, LIVE_P30_UPDATED}:
            sniper_event_type = LIVE_P30_UPDATED if pulse.live_sniper_birth_proven else sniper_event_type
        if sealed_p30:
            sniper_event_type = SNIPER_SEALED_P30_CONFIRMED
        elif sealed_waiting:
            sniper_event_type = P30_DOWNGRADED_TO_R1_WATCH if pulse.live_sniper_birth_proven else SEALED_R1_WATCH_ARMED
        elif sealed_late_center:
            sniper_event_type = SNIPER_SEALED_P30_LATE_CENTER_ONLY
        row = {
            "version": VERSION,
            "event_type": event_type,
            "live_sniper_contract_version": LIVE_SNIPER_CONTRACT_VERSION,
            "live_sniper_event_type": sniper_event_type,
            "live_sniper_event_origin": ORIGIN_LIVE_SOURCE_OBSERVATION if (pulse.live_sniper_birth_proven or live_waiting) else ORIGIN_SEALED_CROSS_BAR if (sealed_p30 or sealed_waiting or sealed_late_center or rejected or cross_failed_at_seal) else None,
            "live_sniper_birth_proven": bool(pulse.live_sniper_birth_proven),
            "live_sniper_p30_birth_proven": bool(pulse.live_sniper_birth_proven),
            "live_sniper_birth_kind": "P30" if pulse.live_sniper_birth_proven else "R1_WATCH_20_30" if bool((pulse.audit or {}).get("r1_watch_mode") == "LIVE_BIRTH_R1") else None,
            "live_sniper_born_at": pulse.live_sniper_born_at or None,
            "live_sniper_source_observation_id": pulse.live_sniper_source_observation_id or None,
            "trader_board_session_date": (pulse.live_sniper_born_at or pulse.first_cross_at or "")[:10] or None,
            "live_sniper_cross_detected_at": (pulse.audit or {}).get("live_sniper_cross_detected_at"),
            "live_sniper_probability_started_at": (pulse.audit or {}).get("live_sniper_probability_started_at"),
            "live_sniper_probability_finished_at": (pulse.audit or {}).get("live_sniper_probability_finished_at"),
            "live_sniper_probability_compute_ms": (pulse.audit or {}).get("live_sniper_probability_compute_ms"),
            "live_sniper_cross_to_probability_ms": (pulse.audit or {}).get("live_sniper_cross_to_probability_ms"),
            "live_sniper_score_kind": (pulse.audit or {}).get("live_sniper_score_kind"),
            "id": pulse.pulse_id, "identifier": pulse.pulse_id,
            "pulse_episode_id": pulse.pulse_id,
            "linked_paac_episode_id": pulse.linked_paac_episode_id or None,
            "linked_paac_signal_bar_time": (pulse.linked_paac_snapshot or {}).get("signal_bar_time") or None,
            "linked_paac_state": (pulse.linked_paac_snapshot or {}).get("state") or None,
            "linked_paac_state_changed_at": (pulse.linked_paac_snapshot or {}).get("state_changed_at") or None,
            "linked_paac_accepted_at": (pulse.linked_paac_snapshot or {}).get("accepted_at") or None,
            "linked_paac_trend_confirmed_at": (pulse.linked_paac_snapshot or {}).get("trend_confirmed_at") or None,
            "linked_paac_pulse_avwap": (pulse.linked_paac_snapshot or {}).get("pulse_avwap"),
            "linked_paac_retention_50": (pulse.linked_paac_snapshot or {}).get("retention_50"),
            "linked_paac_defense_38": (pulse.linked_paac_snapshot or {}).get("defense_38"),
            "linked_paac_anchor_low": (pulse.linked_paac_snapshot or {}).get("anchor_low"),
            "linked_paac_atr14": (pulse.linked_paac_snapshot or {}).get("atr14"),
            "linked_paac_distance_from_cross_atr": (pulse.linked_paac_snapshot or {}).get("distance_from_cross_atr"),
            "linked_paac_bar_extension_atr": (pulse.linked_paac_snapshot or {}).get("bar_extension_atr"),
            "linked_paac_move_since_cross_pct": (pulse.linked_paac_snapshot or {}).get("move_since_cross_pct"),
            "late_no_chase_reason_code": (pulse.audit or {}).get("late_no_chase_reason_code"),
            "market_key": pulse.market_key,
            "decision_market_key": pulse.market_key,
            "decision_lane": "execution" if execution_publishable else "radar",
            "symbol": pulse.symbol,
            "name": pulse.name,
            "timeframe": "30m",
            "source": "R169.3.6.9.3 GANN20 — حلقة موحدة من أول تقاطع حي إلى ختم P30/R1 خلال 20 شمعة",
            "source_file": (pulse.audit or {}).get("source_file"),
            "audit_source_file": (pulse.audit or {}).get("source_file"),
            "source_fields": (pulse.audit or {}).get("source_fields") or 8,
            "recommendation_datetime": pulse.bar_time,
            "signal_bar_time": pulse.bar_time,
            "pulse_bar_time": pulse.bar_time,
            **_bar_window_contract(pulse.audit),
            "signal_detected_at": pulse.first_cross_at,
            "signal_published_at": pulse.appearance_at or None,
            "first_visible_at": pulse.appearance_at or None,
            "attention_visible": bool(pulse.appearance_at),
            "display_threshold_ever_reached": bool(pulse.appearance_at or (pulse.audit or {}).get("display_threshold_ever_reached")),
            "display_threshold_first_reached_at": (pulse.audit or {}).get("display_threshold_first_reached_at") or None,
            "display_threshold_first_reached_scope": (pulse.audit or {}).get("display_threshold_first_reached_scope") or None,
            "attention_eligible_at": (pulse.audit or {}).get("display_threshold_first_reached_at") or pulse.qualified_at or None,
            "first_cross_bar": pulse.bar_time,
            "first_cross_at": pulse.first_cross_at,
            "first_cross_price": pulse.first_cross_price,
            "signal_entry_price": pulse.first_cross_price if pulse.live_sniper_birth_proven else None,
            "entry_signal_price": pulse.first_cross_price if pulse.live_sniper_birth_proven else None,
            "birth_price_frozen": pulse.first_cross_price,
            "birth_source_observation_id": (pulse.audit or {}).get("birth_source_observation_id"),
            "qualified_at": pulse.qualified_at or None,
            "qualification_price": pulse.qualification_price if math.isfinite(pulse.qualification_price) else None,
            "appearance_at": pulse.appearance_at,
            "appearance_price": pulse.appearance_price,
            "current_price": pulse.current_price,
            "signal_bar_close": (pulse.signal_bar_close if (math.isfinite(pulse.signal_bar_close) and not final_score_pending and bool((pulse.audit or {}).get("sealed_close_verified"))) else None),
            "sealed_signal_bar_close": (pulse.signal_bar_close if (math.isfinite(pulse.signal_bar_close) and not final_score_pending and bool((pulse.audit or {}).get("sealed_close_verified"))) else None),
            "signal_close": (pulse.signal_bar_close if (math.isfinite(pulse.signal_bar_close) and not final_score_pending and bool((pulse.audit or {}).get("sealed_close_verified"))) else None),
            **sealed_close_lineage_row_fields(pulse.audit),
            "signal_bar_high": (pulse.signal_bar_high if math.isfinite(pulse.signal_bar_high) else None),
            "signal_bar_sealed": bool(pulse.sealed_at and not final_score_pending and bool((pulse.audit or {}).get("sealed_close_verified"))),
            "signal_bar_sealed_at": (pulse.sealed_at or None) if (not final_score_pending and bool((pulse.audit or {}).get("sealed_close_verified"))) else None,
            "data_state": ("مختوم" if (pulse.sealed_at and not final_score_pending and bool((pulse.audit or {}).get("sealed_close_verified"))) else ("جزئي المصدر" if (pulse.audit or {}).get("sealed_close_reason_code") else "قيد الختم")),
            "target_reached_before_official": target_reached_before_official,
            "target_consumed_before_entry": bool(state == TARGET_CONSUMED_BEFORE_ENTRY or target_reached_before_official),
            "last_update": pulse.last_seen_at,
            "radar_stage": stage,
            "stock_rating": status,
            "status": status,
            "action_state": state,
            "terminal_state": state if state in TERMINAL_STATES else None,
            "monitor_outcome": monitor_outcome,
            "professional_signal_family": "GANN20",
            "professional_signal_type": professional_type,
            "professional_signal_type_ar": stage,
            "visible_signal_case_ar": visible_case,
            "gann20_probability_label": _probability_label(pulse, p50_show, p100_show, sealed_probability_available),
            "gann20_p_r50_pct": p50_show if math.isfinite(p50_show) else None,
            "gann20_p_r100_pct": p100_show if math.isfinite(p100_show) else None,
            "gann20_probability_available": math.isfinite(p50_show),
            "gann20_probability_managed": True,
            "gann20_probability_band": state,
            "gann20_probability_kind": probability_display_kind,
            "live_probability_provisional": bool(live_pending or awaiting or final_score_pending or probability_kind == "PARTIAL_MARKET_PROVISIONAL"),
            "p50_at_cross": pulse.p50_at_first_cross if math.isfinite(pulse.p50_at_first_cross) else None,
            "p100_at_cross": pulse.p100_at_first_cross if math.isfinite(pulse.p100_at_first_cross) else None,
            "p50_at_first_cross": pulse.p50_at_first_cross if math.isfinite(pulse.p50_at_first_cross) else None,
            "p100_at_first_cross": pulse.p100_at_first_cross if math.isfinite(pulse.p100_at_first_cross) else None,
            "p50_at_first_threshold": pulse.p50_at_qualification if math.isfinite(pulse.p50_at_qualification) else None,
            "p100_at_first_threshold": pulse.p100_at_qualification if math.isfinite(pulse.p100_at_qualification) else None,
            "p50_birth": pulse.p50_live if math.isfinite(pulse.p50_live) else None,
            "p100_birth": pulse.p100_live if math.isfinite(pulse.p100_live) else None,
            "p50_live": pulse.p50_live if math.isfinite(pulse.p50_live) else None,
            "p100_live": pulse.p100_live if math.isfinite(pulse.p100_live) else None,
            "p50_live_latest_audit": pulse.p50_live_latest if math.isfinite(pulse.p50_live_latest) else None,
            "p100_live_latest_audit": pulse.p100_live_latest if math.isfinite(pulse.p100_live_latest) else None,
            "p50_final": pulse.p50_final if math.isfinite(pulse.p50_final) else None,
            "p100_final": pulse.p100_final if math.isfinite(pulse.p100_final) else None,
            "p50_sealed": pulse.p50_final if math.isfinite(pulse.p50_final) else None,
            "p100_sealed": pulse.p100_final if math.isfinite(pulse.p100_final) else None,
            "gann20_p_r50_sealed_pct": pulse.p50_final if math.isfinite(pulse.p50_final) else None,
            "gann20_p_r100_sealed_pct": pulse.p100_final if math.isfinite(pulse.p100_final) else None,
            "p50_provisional": pulse.p50_at_qualification if math.isfinite(pulse.p50_at_qualification) else None,
            "p50_provisional_at": pulse.qualified_at or None,
            "probability_scope_first_cross": pulse.probability_scope_first_cross or None,
            "probability_at_cross_exact": bool(math.isfinite(pulse.p50_at_first_cross)),
            "source_observation_id": pulse.live_sniper_source_observation_id or (pulse.audit or {}).get("source_observation_id"),
            "probability_source_observation_id": pulse.live_sniper_source_observation_id or None,
            "probability_bar_time": pulse.bar_time if math.isfinite(pulse.p50_at_first_cross) else None,
            "probability_scope_live": pulse.probability_scope_live or None,
            "probability_scope_sealed": pulse.probability_scope_sealed or None,
            **_probability_display_row_fields(sealed_probability_available, probability_display_scope, probability_display_asof, probability_display_kind),
            "probability_asof_first_cross": pulse.probability_asof_first_cross or None,
            "probability_asof_live": pulse.probability_asof_live or None,
            "probability_asof_sealed": pulse.probability_asof_sealed or None,
            "p50_confirmed": pulse.p50_final if (math.isfinite(pulse.p50_final) and probability_kind == "MARKET_WIDE_CONFIRMED") else None,
            "p50_confirmed_at": pulse.sealed_at if (math.isfinite(pulse.p50_final) and probability_kind == "MARKET_WIDE_CONFIRMED") else None,
            "live_pulse_threshold_pct": pulse.threshold_pct,
            "gann20_contract_version": GANN20_CONTRACT_VERSION,
            "model_horizon_bars": HORIZON_BARS,
            "model_bars_elapsed": int(pulse.model_bars_elapsed or 0),
            "model_bars_remaining": None if state in TERMINAL_STATES else max(0, HORIZON_BARS - int(pulse.model_bars_elapsed or 0)),
            "model_horizon_expired": bool(int(pulse.model_bars_elapsed or 0) >= HORIZON_BARS),
            "sealed_model_anchor_at": pulse.sealed_model_anchor_at or None,
            "sealed_model_anchor_price": pulse.sealed_model_anchor_price if math.isfinite(pulse.sealed_model_anchor_price) else None,
            "r1_clock_restarts_horizon": False,
            "session_reset_restarts_horizon": False,
            "r1_watch_armed": bool((pulse.audit or {}).get("r1_watch_armed") and (live_waiting or sealed_waiting)),
            "r1_watch_armed_at": (pulse.audit or {}).get("r1_watch_armed_at"),
            "r1_watch_mode": (pulse.audit or {}).get("r1_watch_mode") or None,
            "r1_watch_source_observation_id": (pulse.audit or {}).get("r1_watch_source_observation_id") or None,
            "live_r1_birth_proven": bool((pulse.audit or {}).get("live_r1_birth_proven")),
            "gann20_episode_state": state,
            "gann20_episode_outcome": episode_outcome,
            "gann20_episode_outcome_ar": episode_outcome_ar,
            "gann20_episode_terminal": bool(state in TERMINAL_STATES),
            "internal_only": bool(internal_pending or live_waiting or sealed_waiting or sealed_late_center or awaiting),
            "radar_visible": bool((live_pending or sealed_p30) and pulse.live_sniper_birth_proven),
            "signal_bar_is_sealed": bool((sealed_p30 or sealed_waiting or state in TERMINAL_STATES) and bool((pulse.audit or {}).get("sealed_close_verified"))),
            "live_pulse_seal_state": state,
            "seal_failure_reason": seal_failure_reason or None,
            "seal_failure_reason_code": str((pulse.audit or {}).get("seal_failure_reason_code") or "") or None,
            "seal_terminal_probability_status": str((pulse.audit or {}).get("seal_terminal_probability_status") or "") or None,
            "seal_final_score_pending": final_score_pending,
            "live_publishable": publishable,
            "_ain_official": publishable,
            "radar_official_evaluation": publishable,
            "official_rule_passed": publishable,
            "technical_rule_passed": publishable,
            "provisional_rule_passed_before_seal": bool(live_pending or live_waiting or internal_pending or awaiting),
            "two_signal_gate_passed": publishable,
            "tradable": bool(execution_publishable),
            "paper_executable": bool(execution_publishable),
            "is_executable": bool(execution_publishable),
            "professional_actionable": bool(execution_publishable),
            "execution_publishable": bool(execution_publishable),
            "execution_rule_passed": bool(execution_result.get("execution_rule_passed", execution_passed)),
            "execution_shadow_passed": bool(execution_shadow_passed),
            "execution_authority": str(execution_result.get("execution_authority") or "SHADOW_ONLY"),
            "execution_authorized": bool(execution_authorized),
            "entry_status": entry_status,
            "entry_status_code": ("TARGET_CONSUMED_BEFORE_ENTRY" if target_consumed or target_reached_before_official else ("NO_CHASE_REVIEW_ONLY" if no_chase else (execution_result.get("entry_status_code") if execution_result else None))),
            "entry_price": execution_result.get("entry_price") if execution_publishable else None,
            "var3_stop_loss": execution_result.get("stop_price") if execution_publishable else None,
            "take_profit": execution_result.get("target_price") if execution_publishable else None,
            "official_tier": execution_result.get("execution_tier") if execution_publishable else "NONE",
            "official_policy": "R163_GANN20_EPISODE_PLUS_EXECUTION_LAYER",
            "official_policy_version": EXECUTION_LAYER_VERSION,
            "not_for_official_statistics": bool(not publishable),
            "truth_scope": truth_scope,
            "qualified_at_creation": bool((pulse.audit or {}).get("qualified_at_creation")),
            "qualification_state_now": str((pulse.audit or {}).get("qualification_state_now") or "UNKNOWN"),
            "model_available_now": bool((pulse.audit or {}).get("model_available_now", math.isfinite(p50_show))),
            "positive_cross_now": bool((pulse.audit or {}).get("positive_cross_now", False)),
            "activated_r1": bool((pulse.audit or {}).get("activated_r1")),
            "hit_r50": bool((pulse.audit or {}).get("hit_r50")),
            "hit_r100": bool((pulse.audit or {}).get("hit_r100")),
            "cross_bar_hit_r50": bool((pulse.audit or {}).get("cross_bar_hit_r50")),
            "cross_bar_hit_r100": bool((pulse.audit or {}).get("cross_bar_hit_r100")),
            "target_consumed_in_cross_bar": bool((pulse.audit or {}).get("target_consumed_in_cross_bar")),
            "first_r1_crossed_at": (pulse.audit or {}).get("first_r1_crossed_at"),
            "first_r50_hit_at": (pulse.audit or {}).get("first_r50_hit_at"),
            "first_r100_hit_at": (pulse.audit or {}).get("first_r100_hit_at"),
            "r152_price_tape_watch_only_until_seal": bool((live_pending or live_waiting or internal_pending or awaiting or final_score_pending) and not execution_publishable),
            "r153_market_wide_probability_required": True,
            "_monitor_patch_update": True,
            "_r150_live_pulse_patch": True,
            # H12H11 continuity contract: a visible episode never disappears merely
            # because its official/publication eligibility changes while Seal/Terminal
            # work is in flight.  Official-table removal and monitor-row visibility are
            # separate truths.  Hidden internal watches may still leave the monitor;
            # visible finalizing/terminal/review rows remain under the same episode_id.
            "_official_remove_case": bool(
                internal_pending or live_waiting or sealed_waiting or sealed_late_center
                or rejected or cross_failed_at_seal or awaiting or no_chase or failed
                or expired or target_consumed
            ),
            # H12H14: the monitor is an attention projection, not a dump of
            # every background episode.  Never-visible episodes remain durable
            # in engine/history but are absent from the main UI.  Visibility is
            # latched once the model threshold is reached.
            "_monitor_remove_case": bool(not pulse.appearance_at),
            "monitor_projection_continuity": "FINALIZING" if awaiting else ("TERMINAL_VISIBLE" if (no_chase or failed or expired or target_consumed or rejected or cross_failed_at_seal) else "ACTIVE"),
            "var3_gann_category_text": stage if not math.isfinite(p50_show) else (
                f"تقاطع RSIScaled/VAR واحتمال R50/20={p50_show:.2f}%؛ الحالة={stage}"
            ),
            "live_gann_anchor_price": live_levels.get("anchor_price"),
            "live_gann_r1_price": live_levels.get("r1_price"),
            "live_gann_r50_price": live_levels.get("r50_price"),
            "live_gann_r100_price": live_levels.get("r100_price"),
            "live_gann_stop_price": live_levels.get("stop_price"),
            "sealed_gann_anchor_price": sealed_levels.get("anchor_price"),
            "sealed_gann_r1_price": sealed_levels.get("r1_price"),
            "sealed_gann_r50_price": sealed_levels.get("r50_price"),
            "sealed_gann_r100_price": sealed_levels.get("r100_price"),
            "sealed_gann_stop_price": sealed_levels.get("stop_price"),
            "sealed_truth_id": (pulse.audit or {}).get("sealed_truth_id") or None,
            "sealed_truth_complete": bool((pulse.audit or {}).get("sealed_truth_complete")),
            "sealed_truth_missing_fields": list((pulse.audit or {}).get("sealed_truth_missing_fields") or []),
            "effective_truth_scope": (pulse.audit or {}).get("effective_truth_scope") or None,
            "effective_truth_id": (pulse.audit or {}).get("effective_truth_id") or None,
            "effective_truth_contract_version": (pulse.audit or {}).get("effective_truth_contract_version") or None,
            **execution_flat,
            **levels,
        }
        mapping = {
            "anchor_price": "gann_anchor_price",
            "r1_price": "gann_r1_breakout_point",
            "r50_price": "gann_r3_resistance_50",
            "r100_price": "gann_r5_resistance_100",
            "stop_price": "gann_pivot_stop_loss",
        }
        for src, dst in mapping.items():
            if src in levels:
                row[dst] = levels.get(src)
        row = _restore_probability_lineage(row, pulse, sealed_levels, sealed_probability_available, probability_display_scope, probability_display_asof, probability_display_kind)
        row = _truth_apply_trader_columns(row, for_ui=False)
        return _stamp_episode_truth(
            row, truth_source=self._truth_source_for_state(state),
            producer_contract_version=VERSION,
        )


def estimate_live_probability(
    records: Iterable[Mapping[str, Any]], *, market_key: str, symbol: str
) -> Dict[str, Any]:
    """Score one forming/sealed symbol tail for the provisional live gate.

    The production model includes cross-sectional rank/market features.  A fast
    one-symbol score necessarily uses a one-symbol market proxy, so the result is
    explicitly provisional and is replaced by the normal market-wide final score
    at seal.  It is never used directly for Paper execution.
    """
    try:
        import pandas as pd
        from radar30m_live_engine import _add_symbol_features, _add_market_relative_features, _add_pulse_features
        from gann20_probability_model import model_status
        from normalized_gann import build_normalized_gann_grid

        rows: List[Dict[str, Any]] = []
        for raw in records or []:
            r = dict(raw or {})
            dt = pd.to_datetime(r.get("date") or r.get("datetime") or r.get("time"), errors="coerce")
            if pd.isna(dt):
                continue
            try:
                o, h, l, c = [float(r.get(k)) for k in ("open", "high", "low", "close")]
                v = float(r.get("volume") or 0.0)
            except Exception:
                continue
            if not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
                continue
            rows.append({"date": dt, "symbol": str(symbol).upper(), "open": o, "high": max(h,o,l,c), "low": min(l,o,h,c), "close": c, "volume": max(0.0,v)})
        if len(rows) < 12:
            return {"available": False, "error": "insufficient_tail", "failure_stage": "FEATURE_BUILD", "failure_reason_code": "INSUFFICIENT_HISTORY", "feature_count_received": len(rows)}
        t_feature_started = time.perf_counter()
        work = pd.DataFrame(rows).sort_values(["symbol", "date"]).reset_index(drop=True)
        work = _add_symbol_features(work)
        work = _add_market_relative_features(work)
        work = _add_pulse_features(work)
        work, _bundle, _score_status = _score_current_cross_rows_only(work)
        feature_score_ms = (time.perf_counter() - t_feature_started) * 1000.0
        last = work.iloc[-1]
        p50_raw = _finite(last.get("_live_sniper_p50"))
        p100_raw = _finite(last.get("_live_sniper_p100"))
        p50 = p50_raw * 100.0 if math.isfinite(p50_raw) else float("nan")
        p100 = p100_raw * 100.0 if math.isfinite(p100_raw) else float("nan")
        anchor = _finite(last.get("close"))
        if not math.isfinite(p50):
            return _missing_probability_from_scored_row(
                last, feature_count_received=len(rows), sealed=True
            )
        grid = build_normalized_gann_grid(anchor, market_key=str(market_key or ""), symbol=str(symbol or ""), grid_role="r150_live_pulse")
        status = model_status()
        return {
            "available": True,
            "p50_pct": p50,
            "p100_pct": p100,
            "anchor_price": anchor,
            "r1_price": _finite(last.get("gann20_breakout_price"), _finite(grid.get("gann_r1_breakout_point"))),
            "r50_price": _finite(last.get("gann20_target50_price"), _finite(grid.get("gann_r3_resistance_50"))),
            "r100_price": _finite(last.get("gann20_target100_price"), _finite(grid.get("gann_r5_resistance_100"))),
            "stop_price": _finite(grid.get("gann_pivot_stop_loss")),
            "probability_kind": "single_symbol_intrabar_provisional_not_execution_calibrated",
            "model_version": str((status or {}).get("model_version") or ""),
            "probability_score_scope": "CURRENT_CROSS_ROWS_ONLY_ONE_SYMBOL_PROXY",
            "probability_feature_score_ms": round(float(feature_score_ms), 3),
        }
    except Exception as exc:
        _record_stage_error(
            "model_score", "estimate_live_probability", exc,
            market=str(market_key), symbol=str(symbol),
            reason_code="LIVE_PROBABILITY_SCORE_FAILED",
        )
        return {
            "available": False, "error": f"{type(exc).__name__}: {exc}",
            "failure_stage": "MODEL_SCORE",
            "failure_reason_code": "LIVE_PROBABILITY_SCORE_FAILED",
            "error_type": type(exc).__name__, "error_message": str(exc),
        }




def _trim_market_history_for_live_score(frame: Any, *, bars_per_symbol: int = 64) -> Any:
    """Shared exact snapshot contract used by direct and isolated scoring."""
    return _prepare_probability_market_snapshot(frame, bars_per_symbol=bars_per_symbol)


def _a95_score_evidence(
    row: Mapping[str, Any], status: Mapping[str, Any], stage: str, market_key: str,
    work: Any, total_symbols: int, target_symbols: int, started_at: str,
) -> Dict[str, Any]:
    return _a95_build_score_evidence(
        target_row=row, feature_names=list((status or {}).get("feature_names") or []),
        model_status=status, stage=stage, market_key=market_key, market_frame=work,
        target_bar_time=row.get("date"), total_symbols=total_symbols,
        target_bar_symbols=target_symbols, score_started_at=started_at, score_finished_at=_iso(),
    )


def _score_current_cross_rows_only(work: Any) -> tuple[Any, Any, Dict[str, Any]]:
    """Run the production model without historical episode reconstruction.

    This produces the same calibrated P50/P100 model scores for current cross
    rows, but deliberately skips carry-forward, R1 history reconstruction and
    20-bar outcome bookkeeping.  Those belong to the sealed episode ledger and
    must not delay the live sniper birth decision.
    """
    from gann20_probability_model import _add_model_features, _load_bundle, _score_crosses, model_status
    stage: Dict[str, float] = {}
    started = time.perf_counter()
    featured = _add_model_features(work)
    stage["feature_build_ms"] = float(stage.get("feature_build_ms", 0.0) or 0.0) + (time.perf_counter() - started) * 1000.0
    started = time.perf_counter()
    bundle, error = _load_bundle()
    stage["model_load_ms"] = (time.perf_counter() - started) * 1000.0
    if bundle is None:
        return featured, None, {"available": False, "error": str(error or "model_unavailable")}
    started = time.perf_counter()
    p50, p100 = _score_crosses(featured, bundle)
    stage["model_score_ms"] = (time.perf_counter() - started) * 1000.0
    featured = featured.copy()
    featured["_live_sniper_p50"] = p50
    featured["_live_sniper_p100"] = p100
    status = dict(model_status() or {})
    status.update({
        "available": True,
        "error": "",
        "score_scope": "CURRENT_CROSS_ROWS_ONLY",
        "_probability_stage_timing": dict(stage),
    })
    return featured, bundle, status

def estimate_market_live_probability_batch(
    market_df: Any, records_by_symbol: Mapping[str, Iterable[Mapping[str, Any]]], *, market_key: str
) -> Dict[str, Dict[str, Any]]:
    """Batch-score live symbols with one production market feature build."""
    results: Dict[str, Dict[str, Any]] = {}
    _score_started_at = _iso()
    try:
        import pandas as pd
        from radar30m_live_engine import _prepare_df, _add_symbol_features, _add_market_relative_features, _add_pulse_features
        from gann20_probability_model import model_status
        from normalized_gann import build_normalized_gann_grid
        if market_df is None or not hasattr(market_df, "empty") or market_df.empty:
            return {str(k).upper(): {"available": False, "error": "market_snapshot_unavailable"} for k in (records_by_symbol or {})}
        clean: Dict[str, List[Dict[str, Any]]] = {}
        for symbol, records in (records_by_symbol or {}).items():
            sym = str(symbol or "").strip().upper()
            if not sym:
                continue
            rows: List[Dict[str, Any]] = []
            for raw in records or []:
                r = dict(raw or {})
                dt = pd.to_datetime(r.get("date") or r.get("datetime") or r.get("time"), errors="coerce")
                if pd.isna(dt):
                    continue
                try:
                    o, h, l, c = [float(r.get(k)) for k in ("open", "high", "low", "close")]
                    v = float(r.get("volume") or 0.0)
                except Exception:
                    continue
                if not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
                    continue
                rows.append({"date": dt, "symbol": sym, "open": o, "high": max(h,o,l,c), "low": min(l,o,h,c), "close": c, "volume": max(0.0, v), "name": str(r.get("name") or sym)})
            if len(rows) >= 12:
                clean[sym] = rows
            else:
                results[sym] = {"available": False, "error": "insufficient_tail", "failure_stage": "FEATURE_BUILD", "failure_reason_code": "INSUFFICIENT_HISTORY", "feature_count_received": len(rows)}
        if not clean:
            return results
        base = _trim_market_history_for_live_score(market_df, bars_per_symbol=64)
        cols = {str(c).lower(): c for c in base.columns}
        scol = cols.get("symbol")
        if scol is None:
            for sym in clean:
                results[sym] = {"available": False, "error": "market_symbol_column_missing"}
            return results
        syms = set(clean.keys())
        base = base[~base[scol].astype(str).str.upper().isin(syms)].copy()
        fresh_rows: List[Dict[str, Any]] = []
        for rows in clean.values():
            fresh_rows.extend(rows)
        combined = pd.concat([base, pd.DataFrame(fresh_rows)], ignore_index=True, sort=False)
        stage_timing: Dict[str, float] = {"feature_build_ms": 0.0, "model_load_ms": 0.0, "model_score_ms": 0.0}
        t_feature_started = time.perf_counter()
        work = _prepare_df(combined)
        work = _add_symbol_features(work)
        work = _add_market_relative_features(work)
        work = _add_pulse_features(work)
        stage_timing["feature_build_ms"] += (time.perf_counter() - t_feature_started) * 1000.0
        work, _bundle, status = _score_current_cross_rows_only(work)
        for _name, _value in dict((status or {}).get("_probability_stage_timing") or {}).items():
            stage_timing[_name] = float(stage_timing.get(_name, 0.0) or 0.0) + float(_value or 0.0)
        feature_score_ms = sum(float(stage_timing.get(name, 0.0) or 0.0) for name in ("feature_build_ms", "model_load_ms", "model_score_ms"))
        total_symbols = int(work["symbol"].astype(str).nunique()) if "symbol" in work.columns else 0
        for sym in clean:
            target = work[work["symbol"].astype(str).str.upper() == sym].sort_values("date")
            if target.empty:
                results[sym] = {"available": False, "error": "target_missing_after_score"}
                continue
            last = target.iloc[-1]
            p50_raw = _finite(last.get("_live_sniper_p50")); p100_raw = _finite(last.get("_live_sniper_p100"))
            p50 = p50_raw * 100.0 if math.isfinite(p50_raw) else float("nan")
            p100 = p100_raw * 100.0 if math.isfinite(p100_raw) else float("nan")
            anchor = _finite(last.get("close"))
            if not math.isfinite(p50):
                results[sym] = _missing_probability_from_scored_row(
                    last, feature_count_received=len(clean.get(sym) or []), sealed=False
                )
                continue
            grid = build_normalized_gann_grid(anchor, market_key=str(market_key or ""), symbol=sym, grid_role="r159_batch_live_market_gann20")
            target_count = _target_bar_symbol_count(work, last.get("date"))
            results[sym] = {
                "available": True, "p50_pct": p50, "p100_pct": p100,
                "anchor_price": anchor,
                "r1_price": _finite(last.get("gann20_breakout_price"), _finite(grid.get("gann_r1_breakout_point"))),
                "r50_price": _finite(last.get("gann20_target50_price"), _finite(grid.get("gann_r3_resistance_50"))),
                "r100_price": _finite(last.get("gann20_target100_price"), _finite(grid.get("gann_r5_resistance_100"))),
                "stop_price": _finite(grid.get("gann_pivot_stop_loss")),
                "probability_kind": classify_probability_kind(target_count, total_symbols),
                "model_version": str((status or {}).get("model_version") or ""),
                "market_snapshot_symbols": total_symbols,
                "market_target_bar_time": str(last.get("date") or ""),
                "market_target_bar_symbols": target_count,
                "batch_probability": True,
                "probability_score_scope": "CURRENT_CROSS_ROWS_ONLY",
                "probability_feature_score_ms": round(float(feature_score_ms), 3),
                "feature_schema_sha256": str((status or {}).get("feature_schema_sha256") or ""),
                "feature_count_expected": int((status or {}).get("feature_count_expected") or 0),
                "model_sha256": str((status or {}).get("model_sha256") or ""),
                "model_r50_sha256": str((status or {}).get("r50_sha256") or ""),
                "model_r100_sha256": str((status or {}).get("r100_sha256") or ""),
                "probability_feature_vector_sha256": _probability_feature_vector_sha256(
                    last.to_dict(), list((status or {}).get("feature_names") or [])
                ),
                **_a95_score_evidence(last.to_dict(), status, _A95_STAGE_BIRTH, str(market_key or ""), work, total_symbols, target_count, _score_started_at),
            }
        _store_probability_stage_timing(stage_timing)
        return results
    except Exception as exc:
        _store_probability_stage_timing(locals().get("stage_timing", {}))
        _record_stage_error(
            "model_score", "estimate_market_live_probability_batch", exc,
            market=str(market_key), reason_code="MARKET_BATCH_PROBABILITY_FAILED",
        )
        return {
            str(k).upper(): {
                "available": False, "error": f"{type(exc).__name__}: {exc}",
                "failure_stage": "MODEL_SCORE",
                "failure_reason_code": "MARKET_BATCH_PROBABILITY_FAILED",
                "error_type": type(exc).__name__, "error_message": str(exc),
            } for k in (records_by_symbol or {})
        }


def estimate_market_live_probability(
    market_df: Any, records: Iterable[Mapping[str, Any]], *, market_key: str, symbol: str, sealed: bool = False
) -> Dict[str, Any]:
    """Score one forming/sealed bar with production market-wide features."""
    _score_started_at = _iso()
    _score_stage = _a95_score_stage(sealed=bool(sealed))
    try:
        import pandas as pd
        from radar30m_live_engine import _prepare_df, _add_symbol_features, _add_market_relative_features, _add_pulse_features
        from gann20_probability_model import model_status
        from normalized_gann import build_normalized_gann_grid

        if market_df is None or not hasattr(market_df, "empty") or market_df.empty:
            return {"available": False, "error": "market_snapshot_unavailable"}
        sym = str(symbol or "").strip().upper()
        fresh_rows: List[Dict[str, Any]] = []
        for raw in records or []:
            r = dict(raw or {})
            dt = pd.to_datetime(r.get("date") or r.get("datetime") or r.get("time"), errors="coerce")
            if pd.isna(dt):
                continue
            try:
                o, h, l, c = [float(r.get(k)) for k in ("open", "high", "low", "close")]
                v = float(r.get("volume") or 0.0)
            except Exception:
                continue
            if not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
                continue
            fresh_rows.append({"date": dt, "symbol": sym, "open": o, "high": max(h,o,l,c), "low": min(l,o,h,c), "close": c, "volume": max(0.0,v), "name": str(r.get("name") or sym)})
        if len(fresh_rows) < 12:
            return {"available": False, "error": "insufficient_tail", "failure_stage": "FEATURE_BUILD", "failure_reason_code": "INSUFFICIENT_HISTORY", "feature_count_received": len(fresh_rows)}
        base = _trim_market_history_for_live_score(market_df, bars_per_symbol=64)
        cols = {str(c).lower(): c for c in base.columns}
        scol = cols.get("symbol")
        if scol is None:
            return {"available": False, "error": "market_symbol_column_missing"}
        base = base[base[scol].astype(str).str.upper() != sym].copy()
        combined = pd.concat([base, pd.DataFrame(fresh_rows)], ignore_index=True, sort=False)
        stage_timing: Dict[str, float] = {"feature_build_ms": 0.0, "model_load_ms": 0.0, "model_score_ms": 0.0}
        t_feature_started = time.perf_counter()
        work = _prepare_df(combined)
        work = _add_symbol_features(work)
        work = _add_market_relative_features(work)
        work = _add_pulse_features(work)
        stage_timing["feature_build_ms"] += (time.perf_counter() - t_feature_started) * 1000.0
        work, _bundle, _score_status = _score_current_cross_rows_only(work)
        for _name, _value in dict((_score_status or {}).get("_probability_stage_timing") or {}).items():
            stage_timing[_name] = float(stage_timing.get(_name, 0.0) or 0.0) + float(_value or 0.0)
        feature_score_ms = sum(float(stage_timing.get(name, 0.0) or 0.0) for name in ("feature_build_ms", "model_load_ms", "model_score_ms"))
        target = work[work["symbol"].astype(str).str.upper() == sym].sort_values("date")
        if target.empty:
            return {"available": False, "error": "target_missing_after_score"}
        last = target.iloc[-1]
        # The current-cross scorer writes calibrated probabilities into the
        # private decimal columns.  Reading legacy display columns here made every
        # exact sealed-bar score look unavailable even though the model succeeded.
        p50_raw = _finite(last.get("_live_sniper_p50"))
        p100_raw = _finite(last.get("_live_sniper_p100"))
        p50 = p50_raw * 100.0 if math.isfinite(p50_raw) else float("nan")
        p100 = p100_raw * 100.0 if math.isfinite(p100_raw) else float("nan")
        anchor = _finite(last.get("close"))
        if not math.isfinite(p50):
            return _missing_probability_from_scored_row(
                last, feature_count_received=len(fresh_rows), sealed=bool(sealed)
            )
        grid = build_normalized_gann_grid(anchor, market_key=str(market_key or ""), symbol=sym, grid_role="r153_live_market_gann20")
        status = model_status()
        _store_probability_stage_timing(stage_timing)
        return {
            "available": True, "p50_pct": p50, "p100_pct": p100,
            "anchor_price": anchor,
            "r1_price": _finite(last.get("gann20_breakout_price"), _finite(grid.get("gann_r1_breakout_point"))),
            "r50_price": _finite(last.get("gann20_target50_price"), _finite(grid.get("gann_r3_resistance_50"))),
            "r100_price": _finite(last.get("gann20_target100_price"), _finite(grid.get("gann_r5_resistance_100"))),
            "stop_price": _finite(grid.get("gann_pivot_stop_loss")),
            "probability_kind": classify_probability_kind(_target_bar_symbol_count(work, last.get("date")), int(work["symbol"].astype(str).nunique())),
            "model_version": str((status or {}).get("model_version") or ""),
            # Keep the historical symbol count for compatibility, but never use it
            # as fresh-bar coverage.  The target-bar count is the honest cross-section.
            "market_snapshot_symbols": int(work["symbol"].astype(str).nunique()),
            "market_target_bar_time": str(last.get("date") or ""),
            "market_target_bar_symbols": _target_bar_symbol_count(work, last.get("date")),
            "probability_score_scope": "CURRENT_CROSS_ROWS_ONLY",
            "probability_feature_score_ms": round(float(feature_score_ms), 3),
            "feature_schema_sha256": str((_score_status or {}).get("feature_schema_sha256") or ""),
            "feature_count_expected": int((_score_status or {}).get("feature_count_expected") or 0),
            "model_sha256": str((_score_status or {}).get("model_sha256") or ""),
            "model_r50_sha256": str((_score_status or {}).get("r50_sha256") or ""),
            "model_r100_sha256": str((_score_status or {}).get("r100_sha256") or ""),
            "probability_feature_vector_sha256": _probability_feature_vector_sha256(
                last.to_dict(), list((_score_status or {}).get("feature_names") or [])
            ),
            **_a95_score_evidence(last.to_dict(), _score_status or status, _score_stage, str(market_key or ""), work, int(work["symbol"].astype(str).nunique()), _target_bar_symbol_count(work, last.get("date")), _score_started_at),
        }
    except Exception as exc:
        _store_probability_stage_timing(locals().get("stage_timing", {}))
        _record_stage_error(
            "model_score", "estimate_market_live_probability", exc,
            market=str(market_key), symbol=str(symbol),
            reason_code="MARKET_LIVE_PROBABILITY_FAILED",
        )
        return {
            "available": False, "error": f"{type(exc).__name__}: {exc}",
            "failure_stage": "MODEL_SCORE",
            "failure_reason_code": "MARKET_LIVE_PROBABILITY_FAILED",
            "error_type": type(exc).__name__, "error_message": str(exc),
        }
