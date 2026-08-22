# -*- coding: utf-8 -*-
"""Normalize radar review, BIRTH and exact P_SEAL lanes without conflation."""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from sealed_probability_authority import sealed_probability_authority

VERSION = "A96_RADAR_PROBABILITY_LANE_CONTRACT_V1"


def _finite(*values: Any) -> Optional[float]:
    for value in values:
        try:
            out = float(value)
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(out):
            return out
    return None


@dataclass(frozen=True)
class RadarProbabilityLanes:
    row: Dict[str, Any]
    sealed_p50: Optional[float]
    sealed_p100: Optional[float]
    reconstructed_p50: Optional[float]
    reconstructed_p100: Optional[float]
    authority_reason: str


def normalize_probability_lanes(row: Mapping[str, Any]) -> RadarProbabilityLanes:
    out = dict(row or {})
    authority = sealed_probability_authority(out)
    reconstructed_p50 = _finite(out.get("p50_sealed"), out.get("p50_final"), out.get("gann20_p_r50_pct"))
    reconstructed_p100 = _finite(out.get("p100_sealed"), out.get("p100_final"), out.get("gann20_p_r100_pct"))
    sealed_p50 = _finite(out.get("p50_sealed"), out.get("p50_final")) if authority.allowed else None
    sealed_p100 = _finite(out.get("p100_sealed"), out.get("p100_final")) if authority.allowed else None
    if sealed_p50 is None:
        out.pop("p50_sealed", None); out.pop("p50_final", None)
    else:
        out["p50_sealed"] = sealed_p50
    if sealed_p100 is None:
        out.pop("p100_sealed", None); out.pop("p100_final", None)
    else:
        out["p100_sealed"] = sealed_p100
    if reconstructed_p50 is not None:
        out["p50_reconstructed"] = reconstructed_p50
    if reconstructed_p100 is not None:
        out["p100_reconstructed"] = reconstructed_p100
    out["sealed_probability_available"] = sealed_p50 is not None
    out["sealed_probability_authority"] = authority.reason_code
    if not authority.allowed:
        out.update({
            "signal_bar_is_sealed": False,
            "signal_bar_sealed": False,
            "sealed_probability_rejected_reason_code": authority.reason_code,
        })
    return RadarProbabilityLanes(
        out, sealed_p50, sealed_p100, reconstructed_p50,
        reconstructed_p100, authority.reason_code,
    )


__all__ = ["VERSION", "RadarProbabilityLanes", "normalize_probability_lanes"]
