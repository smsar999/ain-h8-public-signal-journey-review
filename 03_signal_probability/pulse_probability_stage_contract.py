# -*- coding: utf-8 -*-
"""Stage-owned probability and GANN geometry extraction for PulseTickTape.

A lifecycle row may carry immutable BIRTH values beside authoritative P_SEAL
values.  This module keeps that precedence explicit and pure so the hot tape
never lets a generic display field overwrite the stage that owns the update.
"""
from __future__ import annotations

import math
import re
from typing import Any, Dict, Iterable, Mapping, Tuple

VERSION = "A4_2_14_H12H14H9_STRUCTURED_PROBABILITY_AUTHORITY_V1"


def _finite(value: Any) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else math.nan
    except (TypeError, ValueError, OverflowError):
        return math.nan


def _first(row: Mapping[str, Any], keys: Iterable[str]) -> float:
    for key in keys:
        value = _finite((row or {}).get(key))
        if math.isfinite(value):
            return value
    return math.nan


def _text(row: Mapping[str, Any], keys: Iterable[str]) -> str:
    return " | ".join(str((row or {}).get(key)) for key in keys if (row or {}).get(key) is not None)


def _probability_keys(*, prefer_sealed: bool) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if prefer_sealed:
        return (
            (
                "p50_sealed", "p50_final", "gann20_p_r50_sealed_pct",
                "gann20_probability_r50_sealed", "sealed_p50_pct",
                "gann20_probability_r50", "gann20_p_r50_pct", "p50_frozen",
            ),
            (
                "p100_sealed", "p100_final", "gann20_p_r100_sealed_pct",
                "gann20_probability_r100_sealed", "sealed_p100_pct",
                "gann20_probability_r100", "gann20_p_r100_pct", "p100_frozen",
            ),
        )
    return (
        (
            "p50_at_first_cross", "p50_birth", "p50_live_first", "p50_live",
            "gann20_probability_r50", "gann20_p_r50_pct", "p50_frozen",
        ),
        (
            "p100_at_first_cross", "p100_birth", "p100_live_first", "p100_live",
            "gann20_probability_r100", "gann20_p_r100_pct", "p100_frozen",
        ),
    )


def extract_probability_authoritative(row: Mapping[str, Any], *, prefer_sealed: bool) -> Tuple[float, float]:
    """Return only structured numeric probability facts.

    Display text is intentionally outside this authority path.  A label such as
    ``R50/20: 88%`` can be rendered by the UI but can never create/arm/update a
    candidate, seal, R1 watch, execution decision or terminal truth.
    """
    source = row or {}
    p50_keys, p100_keys = _probability_keys(prefer_sealed=prefer_sealed)
    return _first(source, p50_keys), _first(source, p100_keys)


def extract_probability_display_only(row: Mapping[str, Any], *, prefer_sealed: bool) -> Tuple[float, float]:
    """Display/diagnostic helper; text-derived values are non-authoritative."""
    source = row or {}
    p50, p100 = extract_probability_authoritative(source, prefer_sealed=prefer_sealed)
    text = _text(source, (
        "gann20_probability_label", "probability", "probability_text",
        "reason", "var3_gann_category_text", "action",
    ))
    if not math.isfinite(p50):
        match = re.search(r"R50(?:/20)?\s*[:=]?\s*([-+]?\d+(?:\.\d+)?)\s*%", text, flags=re.I)
        if match:
            p50 = _finite(match.group(1))
    if not math.isfinite(p100):
        match = re.search(r"R100(?:/20)?\s*[:=]?\s*([-+]?\d+(?:\.\d+)?)\s*%", text, flags=re.I)
        if match:
            p100 = _finite(match.group(1))
    return p50, p100


def extract_probability(row: Mapping[str, Any], *, prefer_sealed: bool) -> Tuple[float, float]:
    """Backward name retained, but authority semantics are now structured-only."""
    return extract_probability_authoritative(row, prefer_sealed=prefer_sealed)


def _reason_level(row: Mapping[str, Any], name: str) -> float:
    text = _text(row, ("reason", "var3_gann_category_text", "action", "status", "radar_stage"))
    match = re.search(rf"(?<!/)\b{name}\s*=\s*([-+]?\d+(?:\.\d+)?)", text, flags=re.I)
    return _finite(match.group(1)) if match else math.nan


def extract_levels(row: Mapping[str, Any], *, prefer_sealed: bool) -> Dict[str, float]:
    source = row or {}

    def pick(*keys: str, reason_name: str = "") -> float:
        value = _first(source, keys)
        if math.isfinite(value):
            return value
        return _reason_level(source, reason_name) if reason_name and not prefer_sealed else math.nan

    if prefer_sealed:
        r1 = pick("sealed_gann_r1_price", "sealed_r1_frozen", "r1_frozen", "gann_r1_breakout_point", "acceptance_breakout_price", "r1")
        r50 = pick("sealed_gann_r50_price", "sealed_r50_frozen", "r50_frozen", "gann_r3_resistance_50", "acceptance_target50_price", "target_price", "r50_price")
        r100 = pick("sealed_gann_r100_price", "sealed_r100_frozen", "r100_frozen", "gann_r5_resistance_100", "r100_price")
        stop = pick("sealed_gann_stop_price", "sealed_stop_frozen", "stop_frozen", "gann_pivot_stop_loss", "var3_stop_loss", "stop_loss", "stop_price")
        anchor = pick("sealed_gann_anchor_price", "sealed_anchor_price", "sealed_model_anchor_price", "gann_anchor_price", "acceptance_anchor_price", "gann_evaluation_price")
        signal_close = pick("sealed_signal_bar_close", "final_signal_bar_close", "signal_bar_close", "signal_close", "bar_close")
    else:
        r1 = pick("live_gann_r1_price", "live_r1_frozen", "r1_frozen", "gann_r1_breakout_point", "acceptance_breakout_price", "breakout_price", "r1", reason_name="R1")
        r50 = pick("live_gann_r50_price", "live_r50_frozen", "r50_frozen", "gann_r3_resistance_50", "acceptance_target50_price", "target_price", "r50_price", reason_name="R50")
        r100 = pick("live_gann_r100_price", "live_r100_frozen", "r100_frozen", "gann_r5_resistance_100", "r100_price", reason_name="R100")
        stop = pick("live_gann_stop_price", "live_stop_frozen", "stop_frozen", "gann_pivot_stop_loss", "var3_stop_loss", "stop_loss", "stop_price")
        anchor = pick("live_gann_anchor_price", "live_anchor_price", "pulse_anchor_price", "gann_anchor_price", "acceptance_anchor_price", "gann_evaluation_price", "entry_price", "appearance_price", "current_price", "close")
        signal_close = pick("signal_bar_close", "signal_close", "bar_close")
    return {
        "r1_frozen": r1, "r50_frozen": r50, "r100_frozen": r100,
        "stop_frozen": stop, "pulse_anchor_price": anchor,
        "appearance_price": pick("appearance_price", "signal_appearance_price", "entry_price", "current_price", "close"),
        "signal_bar_close": signal_close,
    }


__all__ = ["VERSION", "extract_probability", "extract_probability_authoritative", "extract_probability_display_only", "extract_levels"]
