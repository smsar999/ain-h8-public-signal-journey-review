# -*- coding: utf-8 -*-
"""Write-time identity and truth precedence for one live GANN20 episode.

R169.3.6.9 does not change strategy or probability semantics.  It only makes
identity/provenance explicit at the producer boundary and prevents a low-rank
preview row from regressing sealed or terminal truth.
"""
from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, Mapping, Optional, Tuple

from market_datetime_normalizer import canonical_market_time_text as _canonical_market_time_text
from probability_audit_contract import episode_identity

VERSION = "V86CL_R169_3_6_9_3_EPISODE_TRUTH_CONTRACT_V2"

TRUTH_PREVIEW = "PREVIEW"
TRUTH_LIVE_SOURCE = "LIVE_SOURCE_OBSERVATION"
TRUTH_LIVE_TICK = "LIVE_PRICE_TICK"
TRUTH_LIFECYCLE = "LIFECYCLE"
TRUTH_SEALED = "SEALED_CROSS_BAR"
TRUTH_R1_ACTIVATION = "R1_ACTIVATION"
TRUTH_TERMINAL = "TERMINAL_FINALIZER"

TRUTH_RANKS = {
    TRUTH_PREVIEW: 10,
    TRUTH_LIVE_SOURCE: 20,
    TRUTH_LIVE_TICK: 30,
    TRUTH_LIFECYCLE: 40,
    TRUTH_SEALED: 50,
    TRUTH_R1_ACTIVATION: 55,
    TRUTH_TERMINAL: 60,
}

IDENTITY_FIELDS = (
    "episode_id", "episode_key", "episode_key_sha256",
    "episode_market_key", "episode_symbol", "episode_signal_bar_time",
    "episode_detector_family", "episode_timeframe",
)
PROVENANCE_FIELDS = (
    "truth_source", "truth_rank", "truth_stamped_at", "producer_contract_version",
)
VOLATILE_FIELDS = {
    "current_price", "close", "price", "change_pct", "change", "last_update",
    "updated_at", "recorded_at", "tick_price", "tick_high", "tick_low",
    "current_high", "current_low", "movement_since_cross_pct", "move_since_cross_pct",
}
AUTHORITATIVE_FIELDS = {
    "status", "status_ar", "stock_rating", "radar_stage", "monitor_outcome",
    "action_state", "terminal_state", "gann20_episode_state", "live_pulse_seal_state",
    "signal_bar_close", "sealed_signal_bar_close", "signal_close",
    "signal_bar_is_sealed", "signal_bar_sealed", "signal_bar_sealed_at",
    "data_state", "p50_final", "p100_final", "p50_sealed", "p100_sealed",
    "gann20_p_r50_sealed_pct", "gann20_p_r100_sealed_pct",
    "r1_watch_armed", "r1_watch_source", "r1_watch_mode",
    "entry_status", "live_publishable", "published_to_trader", "_ain_official",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _now(value: Any = None) -> str:
    if value not in (None, ""):
        return _text(value)
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _identity_inputs(row: Mapping[str, Any]) -> Tuple[str, str, str, str, str]:
    source = dict(row or {})
    market = _text(source.get("market_key") or source.get("market") or source.get("episode_market_key"))
    symbol = _text(source.get("symbol") or source.get("episode_symbol")).upper()
    bar = _text(
        source.get("signal_bar_time") or source.get("first_cross_bar")
        or source.get("bar_time") or source.get("bar_datetime")
        or source.get("episode_signal_bar_time")
    )
    detector = _text(source.get("detector_family") or source.get("episode_detector_family") or "GANN20").upper()
    timeframe = _text(source.get("timeframe") or source.get("model_timeframe") or source.get("episode_timeframe") or "30M").upper()
    normalized_bar = _canonical_market_time_text(bar, market_key=market, compact=False) or bar
    return market, symbol, normalized_bar, detector, timeframe


def stamp_truth(
    row: Mapping[str, Any], *, truth_source: str, truth_rank: Optional[int] = None,
    stamped_at: Any = None, producer_contract_version: str = VERSION,
    require_identity: bool = True,
) -> Dict[str, Any]:
    """Return a stamped copy.  Missing legal identity is explicit, never guessed later."""
    out = dict(row or {})
    market, symbol, bar, detector, timeframe = _identity_inputs(out)
    if market and symbol and bar:
        identity = episode_identity(
            market, symbol, bar, detector_family=detector, timeframe=timeframe,
        )
        out.setdefault("episode_id", identity["episode_id"])
        out.setdefault("episode_key", identity["episode_key"])
        out.setdefault("episode_key_sha256", identity["episode_key_sha256"])
        out.setdefault("pulse_episode_id", out.get("episode_id"))
        out.setdefault("episode_market_key", market)
        out.setdefault("episode_symbol", symbol)
        out.setdefault("episode_signal_bar_time", bar)
        out.setdefault("episode_detector_family", detector)
        out.setdefault("episode_timeframe", timeframe)
        out.pop("truth_contract_error", None)
    elif require_identity:
        out["truth_contract_error"] = "MISSING_EPISODE_IDENTITY_INPUTS"
    source = _text(truth_source).upper() or TRUTH_PREVIEW
    out["truth_source"] = source
    out["truth_rank"] = int(truth_rank if truth_rank is not None else TRUTH_RANKS.get(source, 0))
    out["truth_stamped_at"] = _now(stamped_at)
    out["producer_contract_version"] = _text(producer_contract_version) or VERSION
    return out


def validate_truth(row: Mapping[str, Any]) -> Tuple[bool, str]:
    source = dict(row or {})
    missing = [field for field in (*IDENTITY_FIELDS[:3], *PROVENANCE_FIELDS) if source.get(field) in (None, "")]
    if missing:
        return False, "MISSING_TRUTH_FIELDS:" + ",".join(missing)
    try:
        int(source.get("truth_rank"))
    except Exception:
        return False, "INVALID_TRUTH_RANK"
    return True, ""


def truth_rank(row: Mapping[str, Any]) -> int:
    try:
        return int((row or {}).get("truth_rank") or 0)
    except Exception:
        return 0


def same_episode(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    a = _text((left or {}).get("episode_key_sha256"))
    b = _text((right or {}).get("episode_key_sha256"))
    if a and b:
        return a == b
    return bool(_text((left or {}).get("episode_id")) and _text((left or {}).get("episode_id")) == _text((right or {}).get("episode_id")))


def _has_episode_identity(row: Mapping[str, Any]) -> bool:
    source = row or {}
    if _text(source.get("truth_contract_error")):
        return False
    return bool(
        _text(source.get("episode_id"))
        and _text(source.get("episode_key_sha256"))
    )


def _block_untrusted_incoming(old: Mapping[str, Any], new: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(old or {})
    out.update({
        "truth_regression_blocked": True,
        "truth_regression_reason": "INCOMING_IDENTITY_INVALID",
        "truth_regression_source": (new or {}).get("truth_source"),
        "truth_regression_rank": truth_rank(new),
        "truth_quarantined": True,
        "truth_quarantine_reason": (new or {}).get("truth_contract_error") or "MISSING_EPISODE_IDENTITY_INPUTS",
    })
    return out


def _merge_lower_rank(old: Mapping[str, Any], new: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(old or {})
    for key in VOLATILE_FIELDS:
        if (new or {}).get(key) not in (None, ""):
            out[key] = (new or {})[key]
    for key, value in (new or {}).items():
        if key in AUTHORITATIVE_FIELDS or key in IDENTITY_FIELDS or key in PROVENANCE_FIELDS:
            continue
        if (key.startswith("debug_") or key.startswith("audit_") or key.startswith("source_")) and value not in (None, ""):
            out[key] = value
    out.update({"truth_regression_blocked": True, "truth_regression_source": (new or {}).get("truth_source"), "truth_regression_rank": truth_rank(new)})
    return out


def merge_truth(existing: Mapping[str, Any], incoming: Mapping[str, Any]) -> Dict[str, Any]:
    """Merge one episode; an identity-less row can never replace known truth."""
    old, new = dict(existing or {}), dict(incoming or {})
    if not old:
        return new
    if not new:
        return old
    old_identified, new_identified = _has_episode_identity(old), _has_episode_identity(new)
    if not new_identified:
        return _block_untrusted_incoming(old, new)
    if not old_identified or not same_episode(old, new):
        return new
    old_rank, new_rank = truth_rank(old), truth_rank(new)
    if new_rank >= old_rank:
        out = dict(old); out.update(new); return out
    return _merge_lower_rank(old, new)


__all__ = [
    "VERSION", "TRUTH_PREVIEW", "TRUTH_LIVE_SOURCE", "TRUTH_LIVE_TICK",
    "TRUTH_LIFECYCLE", "TRUTH_SEALED", "TRUTH_R1_ACTIVATION", "TRUTH_TERMINAL",
    "TRUTH_RANKS", "stamp_truth", "validate_truth", "truth_rank", "same_episode", "merge_truth",
]
