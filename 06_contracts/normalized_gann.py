# -*- coding: utf-8 -*-
"""Normalized Gann acceptance/execution grids for V86AL.

The legacy application used fixed offsets on ``sqrt(price)``.  A fixed root
increment creates very different percentage distances for low- and high-priced
symbols.  This module keeps the square-root coordinate system, but chooses the
root increment from an explicit percentage cell.  Therefore one cell has the
same economic meaning for every symbol.

Important separation
--------------------
* Acceptance grid: frozen from the *sealed pulse close*.  It describes the
  pulse state (watch / armed / breakout / no-chase).  It is never an entry fill.
* Execution grid: created only from the actual next-open fill.  It provides the
  evaluation TP/SL contract and therefore never borrows a future price as a
  pre-entry feature.

This module is deliberately dependency-free and usable by live, historical and
integrity-test routes.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Optional


DEFAULT_CELL_PCT = 0.0125          # one normalized Gann cell = 1.25%
DEFAULT_EVALUATION_FRACTION = 0.5  # half cell = 0.625%
DEFAULT_TARGET50_CELLS = 2.0       # +2.5%
DEFAULT_TARGET100_CELLS = 4.0      # +5.0%
DEFAULT_STOP_CELLS = 1.0           # -1.25%


def _finite_positive(value: Any) -> Optional[float]:
    try:
        out = float(value)
        return out if math.isfinite(out) and out > 0 else None
    except Exception:
        return None


def resolve_tick_size(
    *,
    price: Any,
    market_key: str = "",
    symbol: str = "",
    explicit_tick: Any = None,
) -> float:
    """Return a conservative display/evaluation tick.

    An explicit provider/exchange tick always wins.  The fallback is intentionally
    simple and configurable at call sites; it is not presented as an exchange-rule
    oracle.  It merely prevents impossible floating-point price levels.
    """
    explicit = _finite_positive(explicit_tick)
    if explicit is not None:
        return explicit

    p = _finite_positive(price) or 1.0
    mk = str(market_key or "").lower()
    sym = str(symbol or "").upper()

    if "fx" in mk or "فوركس" in mk or any(x in sym for x in ("USD", "EUR", "GBP", "JPY", "CHF", "AUD", "CAD", "NZD")):
        return 0.01 if "JPY" in sym else 0.0001
    if "crypto" in mk or "عملات رقمية" in mk:
        if p < 1:
            return 0.000001
        if p < 10:
            return 0.0001
        return 0.01
    if p < 1:
        return 0.0001
    return 0.01


def round_to_tick(value: Any, tick_size: Any, direction: str = "nearest") -> float:
    v = _finite_positive(value)
    t = _finite_positive(tick_size)
    if v is None:
        return float("nan")
    if t is None:
        return float(v)
    q = v / t
    mode = str(direction or "nearest").strip().lower()
    if mode == "up":
        units = math.ceil(q - 1e-12)
    elif mode == "down":
        units = math.floor(q + 1e-12)
    else:
        units = round(q)
    # Keep enough precision for FX/crypto while avoiding binary tails.
    decimals = max(0, min(10, int(math.ceil(-math.log10(t))) + 2)) if t < 1 else 4
    return round(max(t, units * t), decimals)


def _up_level(anchor: float, pct: float) -> float:
    """Square-root-coordinate increase producing an exact percentage move."""
    root = math.sqrt(anchor)
    delta = root * (math.sqrt(1.0 + pct) - 1.0)
    return (root + delta) ** 2


def _down_level(anchor: float, pct: float) -> float:
    """Square-root-coordinate decrease producing an exact percentage move."""
    pct = min(max(float(pct), 0.0), 0.999999)
    root = math.sqrt(anchor)
    delta = root * (1.0 - math.sqrt(1.0 - pct))
    return (root - delta) ** 2


@dataclass(frozen=True)
class NormalizedGannConfig:
    cell_pct: float = DEFAULT_CELL_PCT
    evaluation_fraction: float = DEFAULT_EVALUATION_FRACTION
    target50_cells: float = DEFAULT_TARGET50_CELLS
    target100_cells: float = DEFAULT_TARGET100_CELLS
    stop_cells: float = DEFAULT_STOP_CELLS


def build_normalized_gann_grid(
    anchor_price: Any,
    *,
    market_key: str = "",
    symbol: str = "",
    tick_size: Any = None,
    config: Optional[NormalizedGannConfig] = None,
    grid_role: str = "acceptance",
) -> Dict[str, Any]:
    """Build a normalized square-root Gann grid.

    Legacy aliases are included so existing UI columns continue to work:
    ``gann_r1_breakout_point`` = +1 cell, ``gann_r3_resistance_50`` =
    +2 cells and ``gann_pivot_stop_loss`` = -1 cell.
    """
    anchor = _finite_positive(anchor_price)
    cfg = config or NormalizedGannConfig()
    if anchor is None:
        return {
            "gann_grid_valid": False,
            "gann_grid_role": str(grid_role),
            "gann_grid_type": "normalized_percentage_sqrt_v86al",
        }

    cell = max(0.0001, float(cfg.cell_pct))
    eval_pct = cell * max(0.0, float(cfg.evaluation_fraction))
    breakout_pct = cell
    target50_pct = cell * max(1.0, float(cfg.target50_cells))
    target100_pct = cell * max(float(cfg.target50_cells), float(cfg.target100_cells))
    stop_pct = cell * max(0.1, float(cfg.stop_cells))
    support50_pct = cell * max(1.0, float(cfg.target50_cells))
    support100_pct = cell * max(float(cfg.target50_cells), float(cfg.target100_cells))

    tick = resolve_tick_size(
        price=anchor,
        market_key=market_key,
        symbol=symbol,
        explicit_tick=tick_size,
    )

    anchor_tick = round_to_tick(anchor, tick, "nearest")
    breakout = round_to_tick(_up_level(anchor, breakout_pct), tick, "up")
    target50 = round_to_tick(_up_level(anchor, target50_pct), tick, "up")
    target100 = round_to_tick(_up_level(anchor, target100_pct), tick, "up")
    stop = round_to_tick(_down_level(anchor, stop_pct), tick, "down")
    support50 = round_to_tick(_down_level(anchor, support50_pct), tick, "down")
    support100 = round_to_tick(_down_level(anchor, support100_pct), tick, "down")
    evaluation = float((stop + breakout) / 2.0)
    evaluation_effective_pct = float((evaluation / anchor_tick) - 1.0) if anchor_tick > 0 else 0.0
    breakout_effective_pct = float((breakout / anchor_tick) - 1.0) if anchor_tick > 0 else 0.0
    stop_effective_pct = float((stop / anchor_tick) - 1.0) if anchor_tick > 0 else 0.0

    return {
        "gann_grid_valid": True,
        "gann_grid_role": str(grid_role),
        "gann_grid_type": "normalized_percentage_sqrt_v86al",
        "gann_anchor_price": float(anchor_tick),
        "gann_tick_size": float(tick),
        "gann_cell_pct": float(cell),
        "gann_evaluation_fraction": float(cfg.evaluation_fraction),
        "gann_evaluation_price": float(evaluation),
        "gann_r1_breakout_point": float(breakout),
        "gann_r3_resistance_50": float(target50),
        "gann_r5_resistance_100": float(target100),
        "gann_pivot_stop_loss": float(stop),
        "gann_s2_support_50": float(support50),
        "gann_s4_support_100": float(support100),
        "gann_evaluation_pct": float(evaluation_effective_pct),
        "gann_evaluation_nominal_pct": float(eval_pct),
        "gann_evaluation_effective_pct": float(evaluation_effective_pct),
        "gann_breakout_pct": float(breakout_pct),
        "gann_breakout_effective_pct": float(breakout_effective_pct),
        "gann_target50_pct": float(target50_pct),
        "gann_target100_pct": float(target100_pct),
        "gann_stop_pct": float(stop_pct),
        "gann_stop_effective_pct": float(stop_effective_pct),
        # Explicit semantic aliases used by the new radar/lifecycle code.
        "acceptance_anchor_price": float(anchor_tick),
        "acceptance_evaluation_price": float(evaluation),
        "acceptance_breakout_price": float(breakout),
        "acceptance_target50_price": float(target50),
        "acceptance_stop_reference": float(stop),
    }


def build_execution_grid(
    entry_price: Any,
    *,
    market_key: str = "",
    symbol: str = "",
    tick_size: Any = None,
    config: Optional[NormalizedGannConfig] = None,
) -> Dict[str, Any]:
    grid = build_normalized_gann_grid(
        entry_price,
        market_key=market_key,
        symbol=symbol,
        tick_size=tick_size,
        config=config,
        grid_role="execution",
    )
    if not grid.get("gann_grid_valid"):
        return grid
    return {
        **grid,
        "execution_anchor_price": grid["gann_anchor_price"],
        "execution_stop_price": grid["gann_pivot_stop_loss"],
        "execution_target50_price": grid["gann_r3_resistance_50"],
        "execution_target100_price": grid["gann_r5_resistance_100"],
        "execution_fast_silver_price": grid["gann_evaluation_price"],
    }


def classify_acceptance_price(price: Any, grid: Dict[str, Any]) -> str:
    p = _finite_positive(price)
    if p is None or not bool((grid or {}).get("gann_grid_valid")):
        return "INVALID"
    evaluation = _finite_positive(grid.get("gann_evaluation_price"))
    breakout = _finite_positive(grid.get("gann_r1_breakout_point"))
    target50 = _finite_positive(grid.get("gann_r3_resistance_50"))
    if target50 is not None and p >= target50:
        return "TARGET50_OR_BEYOND"
    if breakout is not None and p >= breakout:
        return "BREAKOUT"
    if evaluation is not None and p >= evaluation:
        return "ARMED"
    return "WATCH"


def distance_pct(price: Any, level: Any) -> Optional[float]:
    p = _finite_positive(price)
    l = _finite_positive(level)
    if p is None or l is None:
        return None
    return ((l - p) / p) * 100.0


def evaluate_execution_path(
    bars: Any,
    *,
    entry_price: Any,
    market_key: str = "",
    symbol: str = "",
    tick_size: Any = None,
    horizon_bars: int = 10,
    tie_policy: str = "favor_sl",
    decision_close: Any = None,
) -> Dict[str, Any]:
    """Evaluate historical OHLC with the exact live execution grid.

    The first supplied bar is the actual next-open entry bar.  Contract levels
    come only from :func:`build_execution_grid`; no separate percentage formula
    and no current-close fallback are allowed.
    """
    from radar_contract_policy import evaluate_execution_bar

    entry = _finite_positive(entry_price)
    horizon = max(1, int(horizon_bars or 10))
    tie = str(tie_policy or "favor_sl").strip().lower()
    if entry is None:
        return {
            "path_status": "INVALID_NEXT_OPEN",
            "entry_policy": "next_open_strict_no_current_close_fallback",
            "contract_outcome": "EXCLUDED_INVALID_NEXT_OPEN",
            "historical_live_contract_parity": False,
        }

    grid = build_execution_grid(
        entry,
        market_key=market_key,
        symbol=symbol,
        tick_size=tick_size,
    )
    if not grid.get("gann_grid_valid"):
        return {
            **grid,
            "path_status": "INVALID_EXECUTION_GRID",
            "entry_policy": "next_open_strict_no_current_close_fallback",
            "contract_outcome": "EXCLUDED_INVALID_EXECUTION_GRID",
            "historical_live_contract_parity": False,
        }

    try:
        if hasattr(bars, "head") and hasattr(bars, "iterrows"):
            items = [dict(row) for _, row in bars.head(horizon).iterrows()]
        else:
            items = [dict(row) for row in list(bars or [])[:horizon]]
    except Exception:
        items = []

    valid_items = []
    for row in items:
        try:
            hi = float(row.get("high"))
            lo = float(row.get("low"))
            close = float(row.get("close"))
            if not (math.isfinite(hi) and math.isfinite(lo) and math.isfinite(close)):
                continue
        except Exception:
            continue
        valid_items.append(row)

    if not valid_items:
        return {
            **grid,
            "path_status": "INVALID_FUTURE_OHLC",
            "entry_policy": "next_open_strict_no_current_close_fallback",
            "contract_outcome": "EXCLUDED_INVALID_FUTURE_OHLC",
            "historical_live_contract_parity": True,
        }

    target = float(grid["execution_target50_price"])
    stop = float(grid["execution_stop_price"])
    highs = [float(row["high"]) for row in valid_items]
    lows = [float(row["low"]) for row in valid_items]
    closes = [float(row["close"]) for row in valid_items]
    max_high_full = max(highs)
    min_low_full = min(lows)
    final_close_full = closes[-1]
    full_mfe = max_high_full / entry - 1.0
    full_mae = min_low_full / entry - 1.0
    full_final_ret = final_close_full / entry - 1.0

    first_outcome = "NO_TOUCH"
    first_touch_bar: Any = ""
    first_touch_time: Any = ""
    exit_price = final_close_full
    exit_bar: Any = valid_items[-1].get("date", "")
    full_hit_tp_any = False
    full_hit_sl_any = False

    for idx, row in enumerate(valid_items, 1):
        event = evaluate_execution_bar(
            bar_high=row.get("high"),
            bar_low=row.get("low"),
            bar_close=row.get("close"),
            target_price=target,
            stop_price=stop,
            tie_policy=tie,
        )
        full_hit_tp_any = bool(full_hit_tp_any or event["target_touched"])
        full_hit_sl_any = bool(full_hit_sl_any or event["stop_touched"])
        if first_outcome != "NO_TOUCH" or event["outcome"] == "NO_TOUCH":
            continue
        first_outcome = str(event["outcome"])
        first_touch_bar = idx
        first_touch_time = row.get("date", "")
        exit_bar = row.get("date", "")
        exit_price = float(event["exit_price"])

    exit_count = int(first_touch_bar) if str(first_touch_bar or "").strip().isdigit() else len(valid_items)
    exit_items = valid_items[:max(1, min(exit_count, len(valid_items)))]
    exit_highs = [float(row["high"]) for row in exit_items]
    exit_lows = [float(row["low"]) for row in exit_items]
    mfe = max(exit_highs) / entry - 1.0
    mae = min(exit_lows) / entry - 1.0
    final_ret = float(exit_price) / entry - 1.0
    hit_tp_any = any(float(row["high"]) >= target for row in exit_items)
    hit_sl_any = any(float(row["low"]) <= stop for row in exit_items)

    actual_tp_frac = target / entry - 1.0
    actual_sl_frac = 1.0 - stop / entry
    if first_outcome.startswith("TP"):
        contract_ret = actual_tp_frac
    elif first_outcome.startswith("SL"):
        contract_ret = -actual_sl_frac
    else:
        contract_ret = final_ret
    contract_r = contract_ret / actual_sl_frac if actual_sl_frac > 0 else float("nan")
    dc = _finite_positive(decision_close)

    return {
        **grid,
        "path_status": "OK" if len(valid_items) >= horizon else "PARTIAL_FUTURE_WINDOW",
        "path_engine": "normalized_gann_shared_execution_path_v86am",
        "contract_source": "build_execution_grid_shared_with_live",
        "historical_live_contract_parity": True,
        "entry_policy": "next_open_strict_no_current_close_fallback",
        "current_close_fallback_allowed": False,
        "tie_policy": tie,
        "decision_close": float(dc) if dc is not None else float("nan"),
        "entry_price": float(entry),
        "entry_gap_fraction": (entry / dc - 1.0) if dc is not None else float("nan"),
        "future_bars": int(len(valid_items)),
        "future_window_complete": bool(len(valid_items) >= horizon),
        "mfe_fraction": mfe,
        "mae_fraction": mae,
        "final_return_fraction": final_ret,
        "hit_plus_1pct": bool(mfe >= 0.01),
        "hit_plus_2pct": bool(mfe >= 0.02),
        "hit_minus_1pct": bool(mae <= -0.01),
        "hit_minus_2pct": bool(mae <= -0.02),
        "hit_tp": hit_tp_any,
        "hit_sl": hit_sl_any,
        "full_horizon_mfe_fraction": full_mfe,
        "full_horizon_mae_fraction": full_mae,
        "full_horizon_final_return_fraction": full_final_ret,
        "full_horizon_hit_tp": full_hit_tp_any,
        "full_horizon_hit_sl": full_hit_sl_any,
        "contract_outcome": first_outcome,
        "first_touch_bar": first_touch_bar,
        "first_touch_time": first_touch_time,
        "exit_price": float(exit_price),
        "exit_bar": exit_bar,
        "contract_return_fraction": contract_ret,
        "contract_return_R": contract_r,
        "effective_tp_fraction": actual_tp_frac,
        "effective_sl_fraction": actual_sl_frac,
        "horizon_bars_contract": horizon,
    }
