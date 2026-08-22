# -*- coding: utf-8 -*-
"""V86CL R163 — one source of truth for the GANN20 live/sealed episode contract.

This module deliberately contains no Qt, pandas or model execution.  Every
producer/consumer imports the same immutable contract:

* raw episode identity starts at the first observed positive RSIScaled/VAR3 cross;
* the model anchor is the sealed close of that same 30m cross bar;
* R50/R100 labels use the next 20 completed 30m bars and stop at the earlier
  negative RSIScaled/VAR3 cross;
* P50 >= the market display threshold is a live radar pulse, not a buy order;
* 20 <= sealed P50 < threshold is an internal R1 watch using the frozen sealed
  Gann levels; R1 may emit once within the original 20-bar horizon;
* neither sealing, R1 activation, publication nor a new session restarts the
  20-bar clock.
"""
from __future__ import annotations

import datetime as _dt
import json
import math
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from exception_observability import report_suppressed_exception

VERSION = "V86CL_R163_GANN20_EPISODE_CONTRACT"
TIMEFRAME = "30m"
TIMEFRAME_MINUTES = 30
DEFAULT_HORIZON_BARS = 20
DEFAULT_DISCARD_BELOW_PCT = 20.0
DEFAULT_IMMEDIATE_DISPLAY_PCT = 30.0

PROB_SCOPE_LIVE_CURRENT_BAR = "LIVE_CURRENT_BAR"
PROB_SCOPE_LIVE_CACHED_SAME_BAR = "LIVE_CACHED_SAME_BAR"
PROB_SCOPE_LIVE_STALE_HINT = "LIVE_STALE_HINT"
PROB_SCOPE_SEALED_CROSS_BAR = "SEALED_CROSS_BAR"
PROB_SCOPE_UNAVAILABLE = "UNAVAILABLE"

STATE_RAW_CROSS = "RAW_CROSS"
STATE_LIVE_INTERNAL = "LIVE_INTERNAL_PENDING_SEAL"
STATE_LIVE_P30 = "LIVE_P30_RADAR"
STATE_SEALED_P30 = "SEALED_P30_CONFIRMED"
STATE_SEALED_R1_WATCH = "SEALED_R1_WATCH"
STATE_R1_ACTIVATED = "R1_ACTIVATED"
STATE_REJECTED = "REJECTED"
STATE_EXPIRED = "EXPIRED_20_BARS"
STATE_NEGATIVE_CROSS_CLOSED = "NEGATIVE_CROSS_CLOSED"


def _finite(value: Any, default: float = float("nan")) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.strip().replace("%", "")
            if not value:
                return default
        x = float(value)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _parse_dt(value: Any) -> Optional[_dt.datetime]:
    if isinstance(value, _dt.datetime):
        return value.replace(tzinfo=None)
    try:
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime().replace(tzinfo=None)
    except Exception as exc:
        # Some scalar adapters expose ``to_pydatetime`` but raise while the
        # underlying value is still representable as ISO text.  Preserve the
        # explicit fallback without silently swallowing the failure.
        _conversion_error = f"{type(exc).__name__}:{exc}"
    else:
        _conversion_error = ""
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return _dt.datetime.fromisoformat(text.replace("T", " ").replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def _config_path() -> Path:
    return Path(__file__).resolve().parent / "models" / "v86ck_gann20_probability_config.json"


def load_contract_config() -> Dict[str, Any]:
    try:
        data = json.loads(_config_path().read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_CONFIG = load_contract_config()
_LABEL_CONTRACT = dict(_CONFIG.get("label_contract") or {})
_DISPLAY_POLICY = dict(_CONFIG.get("display_policy") or {})
MODEL_VERSION = str(_CONFIG.get("model_version") or "")


def _configured_horizon_bars() -> int:
    explicit = _DISPLAY_POLICY.get("horizon_bars")
    if explicit not in (None, ""):
        try:
            return max(1, int(explicit))
        except Exception as exc:
            report_suppressed_exception(
                exc, module=__name__, file=__file__,
                function="_configured_horizon_bars", line=113,
                stage="context_write", critical=False,
                reason_code="GANN20_HORIZON_CONFIG_INVALID",
            )
    for text in (
        _LABEL_CONTRACT.get("R50"), _LABEL_CONTRACT.get("R100"),
        (_CONFIG.get("r50") or {}).get("target"), (_CONFIG.get("r100") or {}).get("target"),
    ):
        m = re.search(r"(?:within\s+|_)(\d{1,3})(?:\s+completed|$)", str(text or ""), re.I)
        if m:
            return max(1, int(m.group(1)))
    return DEFAULT_HORIZON_BARS


HORIZON_BARS = _configured_horizon_bars()
DISCARD_BELOW_PCT = float(_DISPLAY_POLICY.get("discard_below_pct") or DEFAULT_DISCARD_BELOW_PCT)
IMMEDIATE_DISPLAY_PCT = float(_DISPLAY_POLICY.get("immediate_display_min_pct") or DEFAULT_IMMEDIATE_DISPLAY_PCT)


def market_family(market_key: str) -> str:
    try:
        from market_key_contract import gann_market_family
        return gann_market_family(market_key)
    except Exception:
        return "generic"


def display_threshold_pct(market_key: str) -> float:
    family = market_family(market_key)
    env_key = {
        "sa": "AIN_LIVE_PULSE_THRESHOLD_SA_PCT",
        "us_local": "AIN_LIVE_PULSE_THRESHOLD_US_LOCAL_PCT",
        "us_api": "AIN_LIVE_PULSE_THRESHOLD_US_API_PCT",
    }.get(family, "AIN_LIVE_PULSE_THRESHOLD_PCT")
    defaults = {"sa": IMMEDIATE_DISPLAY_PCT, "us_local": 40.0, "us_api": 40.0, "generic": IMMEDIATE_DISPLAY_PCT}
    try:
        return max(0.0, min(100.0, float(os.environ.get(env_key, defaults[family]) or defaults[family])))
    except Exception:
        return float(defaults[family])


def probability_band(p50_pct: Any, market_key: str = "") -> str:
    p50 = _finite(p50_pct)
    if not math.isfinite(p50):
        return "UNAVAILABLE"
    threshold = display_threshold_pct(market_key)
    if p50 < DISCARD_BELOW_PCT:
        return "BELOW_20"
    if p50 < 25.0:
        return "INTERNAL_20_25"
    if p50 < threshold:
        return "INTERNAL_25_THRESHOLD"
    return "IMMEDIATE_DISPLAY"


def should_display_live(p50_pct: Any, market_key: str) -> bool:
    p50 = _finite(p50_pct)
    return bool(math.isfinite(p50) and p50 >= display_threshold_pct(market_key))


def should_arm_r1_after_seal(p50_pct: Any, market_key: str) -> bool:
    p50 = _finite(p50_pct)
    threshold = display_threshold_pct(market_key)
    return bool(math.isfinite(p50) and DISCARD_BELOW_PCT <= p50 < threshold)


def canonical_episode_id(market_key: str, symbol: str, first_cross_bar: Any) -> str:
    family = market_family(market_key)
    if family in {"sa", "us_local", "us_api"}:
        market = family
    else:
        raw_market = str(market_key or "market")
        ascii_part = re.sub(r"[^A-Za-z0-9_\-]+", "_", raw_market).strip("_") or "market"
        # Arabic-only/Unicode market names may collapse to the same ASCII slug;
        # include a stable short digest for generic markets to prevent collisions.
        import hashlib
        market = f"{ascii_part}_{hashlib.sha1(raw_market.encode('utf-8')).hexdigest()[:8]}"
    sym = re.sub(r"[^A-Za-z0-9_\-]+", "_", str(symbol or "").upper()).strip("_") or "NA"
    bar = str(first_cross_bar or "").replace(" ", "T").replace(":", "").replace("-", "")
    return f"GANN20-{market}-{sym}-{bar}"


def infer_probability_scope(probability: Mapping[str, Any], observation: Optional[Mapping[str, Any]] = None, *, sealed: bool = False) -> str:
    p = dict(probability or {})
    if sealed:
        return PROB_SCOPE_SEALED_CROSS_BAR
    explicit = str(p.get("probability_scope") or p.get("gann20_probability_scope") or "").strip().upper()
    if not bool(p.get("available")):
        return PROB_SCOPE_UNAVAILABLE
    obs = dict(observation or {})
    obs_bar = str(obs.get("bar_datetime") or obs.get("signal_bar_time") or "")
    prob_bar = str(p.get("probability_bar_time") or p.get("market_target_bar_time") or "")
    obs_id = str(obs.get("source_observation_id") or "")
    prob_obs_id = str(p.get("probability_source_observation_id") or "")
    # A caller-provided LIVE_CURRENT_BAR label is not proof by itself.  When
    # source/bar metadata exist they must identify the exact crossing
    # observation; otherwise a stale same-bar result could be mislabeled live.
    if explicit == PROB_SCOPE_LIVE_CURRENT_BAR:
        if any((obs_bar, prob_bar, obs_id, prob_obs_id)):
            if obs_bar and prob_bar and obs_bar == prob_bar and obs_id and prob_obs_id and obs_id == prob_obs_id:
                return PROB_SCOPE_LIVE_CURRENT_BAR
            return PROB_SCOPE_LIVE_CACHED_SAME_BAR if obs_bar and prob_bar and obs_bar == prob_bar else PROB_SCOPE_LIVE_STALE_HINT
    elif explicit in {
        PROB_SCOPE_LIVE_CACHED_SAME_BAR,
        PROB_SCOPE_LIVE_STALE_HINT,
        PROB_SCOPE_SEALED_CROSS_BAR,
        PROB_SCOPE_UNAVAILABLE,
    }:
        return explicit
    if obs_bar and prob_bar and obs_bar == prob_bar and obs_id and prob_obs_id and obs_id == prob_obs_id:
        return PROB_SCOPE_LIVE_CURRENT_BAR
    if obs_bar and prob_bar and obs_bar == prob_bar:
        return PROB_SCOPE_LIVE_CACHED_SAME_BAR

    # Truth Freeze: official live probability is never inferred from adjacency.
    # A numerically valid probability beside a positive cross is still untrusted
    # unless it carries the exact bar + immutable source-observation identity.
    # Shadow/replay callers may retain the payload as a stale hint, but it cannot
    # create an official live Episode.
    return PROB_SCOPE_LIVE_STALE_HINT


def exact_probability_for_observation(probability: Mapping[str, Any], observation: Mapping[str, Any]) -> bool:
    if not bool((probability or {}).get("available")):
        return False
    return infer_probability_scope(probability, observation) == PROB_SCOPE_LIVE_CURRENT_BAR


def stamp_live_probability(probability: Mapping[str, Any], observation: Mapping[str, Any], *, calculated_at: Any = None) -> Dict[str, Any]:
    out = dict(probability or {})
    obs = dict(observation or {})
    out["probability_scope"] = PROB_SCOPE_LIVE_CURRENT_BAR if bool(out.get("available")) else PROB_SCOPE_UNAVAILABLE
    out["probability_asof"] = str(calculated_at or obs.get("observed_at") or _dt.datetime.now().isoformat(sep=" ", timespec="milliseconds"))
    out["probability_bar_time"] = str(obs.get("bar_datetime") or obs.get("signal_bar_time") or "")
    out["probability_source_observation_id"] = str(obs.get("source_observation_id") or "")
    out["model_horizon_bars"] = HORIZON_BARS
    out["model_anchor_contract"] = str(_LABEL_CONTRACT.get("anchor") or "sealed positive pulse-cross close")
    return out


def stamp_sealed_probability(probability: Mapping[str, Any], *, bar_time: Any, calculated_at: Any = None, source_observation_id: Any = None) -> Dict[str, Any]:
    out = dict(probability or {})
    out["probability_scope"] = PROB_SCOPE_SEALED_CROSS_BAR if bool(out.get("available")) else PROB_SCOPE_UNAVAILABLE
    out["probability_asof"] = str(calculated_at or _dt.datetime.now().isoformat(sep=" ", timespec="milliseconds"))
    out["probability_bar_time"] = str(bar_time or "")
    out["probability_source_observation_id"] = str(source_observation_id or "")
    out["model_horizon_bars"] = HORIZON_BARS
    out["model_anchor_contract"] = str(_LABEL_CONTRACT.get("anchor") or "sealed positive pulse-cross close")
    return out


def contract_fields(*, market_key: str, symbol: str, first_cross_bar: Any, first_cross_at: Any = None, first_cross_price: Any = None,
                    sealed_anchor_at: Any = None, sealed_anchor_price: Any = None, bars_elapsed: int = 0) -> Dict[str, Any]:
    elapsed = max(0, int(bars_elapsed or 0))
    return {
        "gann20_contract_version": VERSION,
        "model_version": MODEL_VERSION,
        "model_timeframe": TIMEFRAME,
        "model_horizon_bars": HORIZON_BARS,
        "model_bars_elapsed": elapsed,
        "model_bars_remaining": max(0, HORIZON_BARS - elapsed),
        "model_horizon_expired": bool(elapsed >= HORIZON_BARS),
        "first_cross_bar": str(first_cross_bar or ""),
        "first_cross_at": str(first_cross_at or ""),
        "first_cross_price": _finite(first_cross_price, None),
        "sealed_model_anchor_at": str(sealed_anchor_at or ""),
        "sealed_model_anchor_price": _finite(sealed_anchor_price, None),
        "pulse_episode_id": canonical_episode_id(market_key, symbol, first_cross_bar),
        "r1_clock_restarts_horizon": False,
        "session_reset_restarts_horizon": False,
        "label_r50_contract": str(_LABEL_CONTRACT.get("R50") or ""),
        "label_r100_contract": str(_LABEL_CONTRACT.get("R100") or ""),
    }


@dataclass(frozen=True)
class EpisodePolicy:
    horizon_bars: int = HORIZON_BARS
    discard_below_pct: float = DISCARD_BELOW_PCT
    immediate_display_pct: float = IMMEDIATE_DISPLAY_PCT


POLICY = EpisodePolicy()

__all__ = [
    "VERSION", "MODEL_VERSION", "TIMEFRAME", "TIMEFRAME_MINUTES", "HORIZON_BARS",
    "DISCARD_BELOW_PCT", "IMMEDIATE_DISPLAY_PCT", "POLICY",
    "PROB_SCOPE_LIVE_CURRENT_BAR", "PROB_SCOPE_LIVE_CACHED_SAME_BAR",
    "PROB_SCOPE_LIVE_STALE_HINT", "PROB_SCOPE_SEALED_CROSS_BAR", "PROB_SCOPE_UNAVAILABLE",
    "STATE_RAW_CROSS", "STATE_LIVE_INTERNAL", "STATE_LIVE_P30", "STATE_SEALED_P30",
    "STATE_SEALED_R1_WATCH", "STATE_R1_ACTIVATED", "STATE_REJECTED", "STATE_EXPIRED",
    "STATE_NEGATIVE_CROSS_CLOSED", "display_threshold_pct", "probability_band",
    "should_display_live", "should_arm_r1_after_seal", "canonical_episode_id",
    "infer_probability_scope", "exact_probability_for_observation", "stamp_live_probability",
    "stamp_sealed_probability", "contract_fields", "load_contract_config", "market_family",
]
