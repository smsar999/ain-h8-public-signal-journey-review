# -*- coding: utf-8 -*-
"""Authoritative probability/close projection for PulseTickTape R1 events.

A sealed R1 event is a new view of an already sealed episode.  It must inherit
that episode's exact close lineage and P_SEAL values instead of falling back to
a forming-row placeholder.  Live-birth R1 events keep the immutable BIRTH
score and do not claim a sealed close.
"""
from __future__ import annotations

import math
from typing import Any, Dict, Mapping

VERSION = "A100_R1_EVENT_TRUTH_PROJECTION_V2"

_SEALED_LINEAGE_KEYS = (
    "sealed_close_verified",
    "sealed_close_source",
    "sealed_source_bar_time",
    "sealed_close_source_observation_id",
    "sealed_close_verified_at",
    "sealed_close_reason_code",
    "signal_bar_sealed_at",
    "forming_window_start",
    "forming_window_end",
    "bar_label_mode",
    "probability_asof_sealed",
    "probability_bar_time",
    "source_snapshot_id",
    "source_tail_sha256",
)


def _finite(value: Any) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return math.nan
    return number if math.isfinite(number) else math.nan



def _first_value(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None

def _label(p50: float, p100: float) -> str:
    if not math.isfinite(p50):
        return ""
    if math.isfinite(p100):
        return f"R50 {p50:.1f}% | R100 {p100:.1f}%"
    return f"R50 {p50:.1f}%"


def sealed_candidate_patch(
    row: Mapping[str, Any], levels: Mapping[str, Any], state: Mapping[str, Any], *,
    p50: float, p100: float,
) -> Dict[str, Any]:
    """Build the complete authoritative sealed patch stored on a candidate."""
    source = dict(row or {})
    geometry = dict(levels or {})
    previous = dict(state or {})
    close = _finite(_first_value(
        source.get("sealed_signal_bar_close"),
        source.get("signal_bar_close"),
        geometry.get("signal_bar_close"),
    ))
    patch: Dict[str, Any] = {
        "p50_sealed": p50,
        "p100_sealed": p100,
        "signal_bar_close": close if math.isfinite(close) else previous.get("signal_bar_close"),
        "sealed_signal_bar_close": close if math.isfinite(close) else previous.get("sealed_signal_bar_close"),
        "signal_close": close if math.isfinite(close) else previous.get("signal_close"),
        "signal_bar_close_state": source.get("signal_bar_close_state") or ("SEALED_VERIFIED" if math.isfinite(close) else previous.get("signal_bar_close_state")),
        "signal_bar_close_source": source.get("signal_bar_close_source") or ("SEALED_CROSS_BAR" if math.isfinite(close) else previous.get("signal_bar_close_source")),
        "signal_bar_is_sealed": bool(source.get("signal_bar_is_sealed") or source.get("signal_bar_sealed") or math.isfinite(close)),
        "signal_bar_sealed": bool(source.get("signal_bar_sealed") or source.get("signal_bar_is_sealed") or math.isfinite(close)),
        "data_state": source.get("data_state") or ("مختوم" if bool(source.get("sealed_close_verified")) and math.isfinite(close) else previous.get("data_state")),
        "sealed_model_anchor_at": source.get("sealed_model_anchor_at") or source.get("sealed_at") or source.get("last_update"),
        "sealed_model_anchor_price": source.get("sealed_model_anchor_price") if source.get("sealed_model_anchor_price") not in (None, "") else geometry.get("signal_bar_close"),
        "sealed_r1_frozen": source.get("sealed_gann_r1_price") if source.get("sealed_gann_r1_price") not in (None, "") else geometry.get("r1_frozen"),
        "sealed_r50_frozen": source.get("sealed_gann_r50_price") if source.get("sealed_gann_r50_price") not in (None, "") else geometry.get("r50_frozen"),
        "sealed_r100_frozen": source.get("sealed_gann_r100_price") if source.get("sealed_gann_r100_price") not in (None, "") else geometry.get("r100_frozen"),
        "sealed_stop_frozen": source.get("sealed_gann_stop_price") if source.get("sealed_gann_stop_price") not in (None, "") else geometry.get("stop_frozen"),
        "sealed_anchor_price": source.get("sealed_gann_anchor_price") if source.get("sealed_gann_anchor_price") not in (None, "") else geometry.get("pulse_anchor_price"),
    }
    for key in _SEALED_LINEAGE_KEYS:
        value = source.get(key)
        if value not in (None, ""):
            patch[key] = value
    return patch


def r1_event_truth(
    candidate: Mapping[str, Any], *, live_r1_watch: bool, sealed_r1_watch: bool,
    live_scope: str, sealed_scope: str,
) -> Dict[str, Any]:
    """Project the correct score stage and exact sealed-close proof to an R1 event."""
    cand = dict(candidate or {})
    if sealed_r1_watch:
        p50 = _finite(cand.get("p50_sealed"))
        p100 = _finite(cand.get("p100_sealed"))
        close = _finite(_first_value(cand.get("sealed_signal_bar_close"), cand.get("signal_bar_close")))
        verified = bool(cand.get("sealed_close_verified")) and math.isfinite(close)
        patch: Dict[str, Any] = {
            "p50_sealed": p50 if math.isfinite(p50) else None,
            "p100_sealed": p100 if math.isfinite(p100) else None,
            "gann20_p_r50_pct": p50 if math.isfinite(p50) else None,
            "gann20_p_r100_pct": p100 if math.isfinite(p100) else None,
            "gann20_probability_pct": p50 if math.isfinite(p50) else None,
            "gann20_probability_label": _label(p50, p100),
            "probability_scope": sealed_scope,
            "probability_scope_sealed": sealed_scope,
            "probability_kind": sealed_scope,
            "signal_bar_close": close if verified else None,
            "sealed_signal_bar_close": close if verified else None,
            "signal_close": close if verified else None,
            "signal_bar_close_state": cand.get("signal_bar_close_state") or ("SEALED_VERIFIED" if verified else "SOURCE_UNAVAILABLE"),
            "signal_bar_close_source": cand.get("signal_bar_close_source") or ("SEALED_CROSS_BAR" if verified else ""),
            "signal_bar_is_sealed": bool(verified),
            "signal_bar_sealed": bool(verified),
            "sealed_close_verified": bool(verified),
            "data_state": "مختوم" if verified else "جزئي المصدر",
        }
        for key in _SEALED_LINEAGE_KEYS:
            if cand.get(key) not in (None, ""):
                patch[key] = cand.get(key)
        return patch

    p50 = _finite(cand.get("p50_at_first_cross", cand.get("p50_frozen")))
    p100 = _finite(cand.get("p100_at_first_cross", cand.get("p100_frozen")))
    return {
        "p50_sealed": cand.get("p50_sealed"),
        "p100_sealed": cand.get("p100_sealed"),
        "gann20_p_r50_pct": p50 if math.isfinite(p50) else None,
        "gann20_p_r100_pct": p100 if math.isfinite(p100) else None,
        "gann20_probability_pct": p50 if math.isfinite(p50) else None,
        "gann20_probability_label": _label(p50, p100),
        "probability_scope": live_scope,
        "probability_scope_sealed": None,
        "probability_kind": "BIRTH_INTRABAR_PROVISIONAL" if live_r1_watch else "",
        "signal_bar_close": cand.get("signal_bar_close"),
    }


__all__ = ["VERSION", "sealed_candidate_patch", "r1_event_truth"]
