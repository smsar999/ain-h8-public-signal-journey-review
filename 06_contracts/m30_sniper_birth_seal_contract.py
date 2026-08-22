# -*- coding: utf-8 -*-
"""R169.3.6.8 operational sniper policy layered above frozen GANN20 semantics.

This module deliberately does not change the trained GANN20 contract, thresholds,
probability model, or 20-bar label.  It only defines when an exact immutable
BIRTH probability in the existing 20-30 band may arm a hidden live R1 watch.
"""
from __future__ import annotations

import math
from typing import Any

from gann20_episode_contract import DISCARD_BELOW_PCT, display_threshold_pct

VERSION = "V86CL_R169_3_6_8_M30_SNIPER_BIRTH_SEAL_POLICY"


def should_arm_live_r1_at_birth(p50_pct: Any, market_key: str) -> bool:
    """Return True for the existing 20-to-display-threshold probability band.

    Callers must additionally prove that the score belongs to the same immutable
    source_observation_id that created the positive RSIScaled/VAR3 cross.
    """
    try:
        p50 = float(p50_pct)
    except (TypeError, ValueError):
        return False
    return bool(
        math.isfinite(p50)
        and float(DISCARD_BELOW_PCT) <= p50 < float(display_threshold_pct(market_key))
    )


__all__ = ["VERSION", "should_arm_live_r1_at_birth"]
