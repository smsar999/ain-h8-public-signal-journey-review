# -*- coding: utf-8 -*-
"""A98 exact probability hot path with per-market incremental feature caching.

The production GANN20 model and its 29-feature schema are unchanged.  This module
only removes repeated work:

* per-symbol rolling/pulse/model-local features are cached by a causal snapshot
  signature and reused when the symbol history is unchanged;
* cross-sectional ranks/market aggregates are rebuilt only for the target bars;
* only the requested target rows are passed to LightGBM.

Every cache is scoped by ``market_key``.  Saudi, US, Forex and any future market
therefore remain isolated.  The fast path is continuously parity-tested against
``live_pulse_seal_engine.estimate_market_live_probability*``.
"""
from __future__ import annotations

import hashlib
import math
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

ProgressCallback = Optional[Callable[[str, Mapping[str, Any]], None]]

CACHE_CONTRACT_VERSION = "A98_MARKET_FEATURE_CACHE_V1"
DEFAULT_MAX_MARKETS = max(1, min(32, int(os.environ.get("AIN_PROBABILITY_FEATURE_CACHE_MARKETS", "8") or 8)))


def _progress(callback: ProgressCallback, stage: str, **details: Any) -> None:
    if callback is None:
        return
    try:
        callback(str(stage), dict(details))
    except Exception:
        # Progress reporting must never affect the score.
        return


def _finite(value: Any, default: float = float("nan")) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _clean_records(
    records_by_symbol: Mapping[str, Iterable[Mapping[str, Any]]],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Dict[str, Dict[str, Any]]]:
    clean: Dict[str, List[Dict[str, Any]]] = {}
    failures: Dict[str, Dict[str, Any]] = {}
    for symbol, records in (records_by_symbol or {}).items():
        sym = str(symbol or "").strip().upper()
        if not sym:
            continue
        rows: List[Dict[str, Any]] = []
        for raw in records or []:
            row = dict(raw or {})
            dt = pd.to_datetime(row.get("date") or row.get("datetime") or row.get("time"), errors="coerce")
            if pd.isna(dt):
                continue
            try:
                o, h, l, c = [float(row.get(key)) for key in ("open", "high", "low", "close")]
                volume = float(row.get("volume") or 0.0)
            except (TypeError, ValueError, OverflowError):
                continue
            if not all(math.isfinite(value) and value > 0.0 for value in (o, h, l, c)):
                continue
            rows.append({
                "date": dt,
                "symbol": sym,
                "open": o,
                "high": max(h, o, l, c),
                "low": min(l, o, h, c),
                "close": c,
                "volume": max(0.0, volume),
                "name": str(row.get("name") or sym),
            })
        if len(rows) >= 12:
            clean[sym] = rows
        else:
            failures[sym] = {
                "available": False,
                "error": "insufficient_tail",
                "failure_stage": "FEATURE_BUILD",
                "failure_reason_code": "INSUFFICIENT_HISTORY",
                "feature_count_received": len(rows),
            }
    return clean, failures


def _symbol_groups_and_signatures(frame: pd.DataFrame) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
    """Hash all rows once, then derive exact per-symbol signatures cheaply."""
    if frame is None or frame.empty:
        return {}, {}
    columns = [name for name in ("date", "symbol", "open", "high", "low", "close", "volume", "name") if name in frame.columns]
    row_hashes = pd.util.hash_pandas_object(frame[columns], index=False, categorize=True).to_numpy(dtype="uint64")
    groups: Dict[str, pd.DataFrame] = {}
    signatures: Dict[str, str] = {}
    for symbol, positions in frame.groupby("symbol", sort=False).indices.items():
        pos = np.asarray(positions, dtype=int)
        key = str(symbol)
        groups[key] = frame.iloc[pos].copy()
        signatures[key] = hashlib.sha256(row_hashes[pos].tobytes()).hexdigest()
    return groups, signatures


def _add_model_local_features(work: pd.DataFrame) -> pd.DataFrame:
    """Exact vectorized per-symbol portion of _add_model_features."""
    out = work.copy()
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    keys = out["symbol"]
    grouped = out.groupby("symbol", sort=False)

    gap = pd.to_numeric(out.get("pulse_gap_pct"), errors="coerce")
    out["pulse_gap_slope1"] = gap - grouped["pulse_gap_pct"].shift(1)
    out["pulse_gap_slope3"] = gap - grouped["pulse_gap_pct"].shift(3)

    below = (
        pd.to_numeric(out.get("RSIScaled"), errors="coerce")
        < pd.to_numeric(out.get("var3"), errors="coerce")
    ).fillna(False)
    reset_group = (~below).groupby(keys, sort=False).cumsum()
    run = below.astype(int).groupby([keys, reset_group], sort=False).cumsum().astype(float)
    out["pre_negative_age"] = run.groupby(keys, sort=False).shift(1)

    shifted_ret1 = grouped["ret1"].shift(1)
    out["ret1_std8"] = (
        shifted_ret1.groupby(keys, sort=False).rolling(8, min_periods=4).std()
        .reset_index(level=0, drop=True).sort_index()
    )
    out["ret1_std20"] = (
        shifted_ret1.groupby(keys, sort=False).rolling(20, min_periods=8).std()
        .reset_index(level=0, drop=True).sort_index()
    )

    high = pd.to_numeric(out.get("high"), errors="coerce")
    low = pd.to_numeric(out.get("low"), errors="coerce")
    previous_close = pd.to_numeric(out.get("prev_close"), errors="coerce")
    true_range = np.maximum.reduce([
        (high - low).abs().to_numpy(dtype=float),
        (high - previous_close).abs().to_numpy(dtype=float),
        (low - previous_close).abs().to_numpy(dtype=float),
    ])
    tr_series = pd.Series(true_range, index=out.index, dtype=float)
    shifted_tr = tr_series.groupby(keys, sort=False).shift(1)
    atr = (
        shifted_tr.groupby(keys, sort=False).rolling(14, min_periods=6).mean()
        .reset_index(level=0, drop=True).sort_index()
    )
    out["atr14_pct"] = atr / pd.to_numeric(out.get("close"), errors="coerce").replace(0, np.nan)

    rolling_low = (
        pd.to_numeric(out.get("low"), errors="coerce")
        .groupby(keys, sort=False).rolling(20, min_periods=5).min()
        .reset_index(level=0, drop=True).sort_index()
    )
    out["prev_low20"] = rolling_low.groupby(keys, sort=False).shift(1)
    out["range_position20"] = (
        (pd.to_numeric(out.get("close"), errors="coerce") - out["prev_low20"])
        / (pd.to_numeric(out.get("prev_high20"), errors="coerce") - out["prev_low20"]).replace(0, np.nan)
    )
    out["close_vs_prev_high20"] = (
        pd.to_numeric(out.get("close"), errors="coerce")
        / pd.to_numeric(out.get("prev_high20"), errors="coerce").replace(0, np.nan)
        - 1.0
    )
    out["volume_log_ratio20"] = np.log1p(
        pd.to_numeric(out.get("volume_ratio20"), errors="coerce").clip(lower=0, upper=50)
    )

    hour = out["date"].dt.hour + out["date"].dt.minute / 60.0
    out["tod_sin"] = np.sin(2.0 * np.pi * (hour - 10.0) / 6.0)
    out["tod_cos"] = np.cos(2.0 * np.pi * (hour - 10.0) / 6.0)
    return out


def _add_target_cross_section(local_work: pd.DataFrame, target_dates: Sequence[Any]) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """Exact cross-sectional portion of _add_model_features for target dates only."""
    dates = pd.to_datetime(pd.Series(list(target_dates)), errors="coerce").dropna().unique()
    if len(dates) == 0:
        return local_work.iloc[0:0].copy(), {}
    date_values = pd.to_datetime(local_work["date"], errors="coerce")
    target_slice = local_work[date_values.isin(dates)].copy()
    if target_slice.empty:
        return target_slice, {}
    by_date = target_slice.groupby("date", sort=True)
    for column in ("ret1", "ret4", "ret8", "volume_ratio20", "air_room40"):
        target_slice[f"rank_{column}"] = by_date[column].rank(pct=True)
    market = by_date.agg(
        market_ret1=("ret1", "mean"),
        market_ret4=("ret4", "mean"),
        market_ret8=("ret8", "mean"),
        market_breadth_pos=("ret1", lambda series: float((pd.to_numeric(series, errors="coerce") > 0).mean())),
        market_new_cross=("technical_signal_live", lambda series: float(pd.Series(series).fillna(False).astype(bool).mean())),
        market_volume_ratio=("volume_ratio20", "median"),
    ).reset_index()
    target_slice = target_slice.merge(market, on="date", how="left", sort=False)
    target_slice = target_slice.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)
    counts = {
        str(pd.Timestamp(key)): int(value)
        for key, value in target_slice.groupby("date", sort=False)["symbol"].nunique().items()
    }
    return target_slice, counts


def _build_local(frame: pd.DataFrame) -> pd.DataFrame:
    from radar30m_live_engine import _add_pulse_features, _add_symbol_features, _prepare_df

    work = _prepare_df(frame)
    work = _add_symbol_features(work)
    work = _add_pulse_features(work)
    work = _add_model_local_features(work)
    return work.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)


@dataclass
class _MarketState:
    signatures: Dict[str, str] = field(default_factory=dict)
    frames: Dict[str, pd.DataFrame] = field(default_factory=dict)
    assembled: Optional[pd.DataFrame] = None
    snapshot_key: str = ""
    last_used_monotonic: float = 0.0
    generations: int = 0


class ProbabilityMarketFeatureCache:
    """Incremental, market-isolated cache for production probability features."""

    def __init__(self, max_markets: int = DEFAULT_MAX_MARKETS) -> None:
        self.max_markets = max(1, int(max_markets))
        self._lock = threading.RLock()
        self._markets: "OrderedDict[str, _MarketState]" = OrderedDict()
        self._stats: Dict[str, int] = {
            "requests": 0,
            "exact_snapshot_hits": 0,
            "incremental_hits": 0,
            "full_builds": 0,
            "symbols_reused": 0,
            "symbols_rebuilt": 0,
        }

    def clear(self) -> None:
        with self._lock:
            self._markets.clear()

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "contract_version": CACHE_CONTRACT_VERSION,
                "markets": {
                    market: {
                        "symbols": len(state.frames),
                        "snapshot_key": state.snapshot_key,
                        "generations": state.generations,
                    }
                    for market, state in self._markets.items()
                },
                "stats": dict(self._stats),
            }

    def _state(self, market_key: str) -> _MarketState:
        market = str(market_key or "").strip() or "UNKNOWN"
        state = self._markets.get(market)
        if state is None:
            state = _MarketState()
            self._markets[market] = state
        self._markets.move_to_end(market)
        while len(self._markets) > self.max_markets:
            self._markets.popitem(last=False)
        return state

    def base_local(
        self,
        market_df: Any,
        *,
        market_key: str,
        snapshot_key: str = "",
        progress: ProgressCallback = None,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        started = time.perf_counter()
        from live_pulse_seal_engine import _trim_market_history_for_live_score
        from radar30m_live_engine import _prepare_df

        market = str(market_key or "").strip() or "UNKNOWN"
        key = str(snapshot_key or "")
        with self._lock:
            self._stats["requests"] += 1
            state = self._state(market)
            if key and state.snapshot_key == key and state.assembled is not None:
                self._stats["exact_snapshot_hits"] += 1
                state.last_used_monotonic = time.monotonic()
                return state.assembled, {
                    "feature_cache_contract_version": CACHE_CONTRACT_VERSION,
                    "feature_cache_hit": True,
                    "feature_cache_exact_snapshot_hit": True,
                    "feature_cache_reused_symbols": len(state.frames),
                    "feature_cache_rebuilt_symbols": 0,
                    "feature_cache_prepare_ms": round((time.perf_counter() - started) * 1000.0, 3),
                    "feature_cache_market_key": market,
                }
            old_signatures = dict(state.signatures)
            old_frames = dict(state.frames)

        prepared = _trim_market_history_for_live_score(market_df, bars_per_symbol=64)
        normalized = _prepare_df(prepared)
        _progress(progress, "FEATURE_CACHE_SIGNATURE", market_key=market, rows=len(normalized))
        groups, signatures = _symbol_groups_and_signatures(normalized)

        reused = [symbol for symbol, signature in signatures.items() if old_signatures.get(symbol) == signature and symbol in old_frames]
        changed = [symbol for symbol in groups if symbol not in reused]
        _progress(progress, "FEATURE_CACHE_BUILD", market_key=market, reused=len(reused), changed=len(changed))

        new_frames: Dict[str, pd.DataFrame] = {symbol: old_frames[symbol] for symbol in reused}
        if changed:
            changed_raw = pd.concat([groups[symbol] for symbol in sorted(changed)], ignore_index=True, sort=False)
            changed_featured = _build_local(changed_raw)
            for symbol, frame in changed_featured.groupby("symbol", sort=False):
                new_frames[str(symbol)] = frame.copy()
        assembled = (
            pd.concat([new_frames[symbol] for symbol in sorted(new_frames)], ignore_index=True, sort=False)
            if new_frames else normalized.iloc[0:0].copy()
        )
        assembled = assembled.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)
        with self._lock:
            state = self._state(market)
            state.signatures = signatures
            state.frames = new_frames
            state.assembled = assembled
            state.snapshot_key = key
            state.last_used_monotonic = time.monotonic()
            state.generations += 1
            self._stats["symbols_reused"] += len(reused)
            self._stats["symbols_rebuilt"] += len(changed)
            if reused:
                self._stats["incremental_hits"] += 1
            else:
                self._stats["full_builds"] += 1
        return assembled, {
            "feature_cache_contract_version": CACHE_CONTRACT_VERSION,
            "feature_cache_hit": bool(reused),
            "feature_cache_exact_snapshot_hit": False,
            "feature_cache_reused_symbols": len(reused),
            "feature_cache_rebuilt_symbols": len(changed),
            "feature_cache_prepare_ms": round((time.perf_counter() - started) * 1000.0, 3),
            "feature_cache_market_key": market,
        }


_GLOBAL_CACHE = ProbabilityMarketFeatureCache()


def cache_snapshot() -> Dict[str, Any]:
    return _GLOBAL_CACHE.snapshot()


def clear_cache() -> None:
    _GLOBAL_CACHE.clear()


def _score_target_rows(
    combined_local: pd.DataFrame,
    target_symbols: Sequence[str],
    *,
    progress: ProgressCallback = None,
) -> Tuple[pd.DataFrame, Dict[str, Any], Dict[str, Any]]:
    from gann20_probability_model import _load_bundle, _platt_calibrate, model_status

    symbols = {str(symbol or "").strip().upper() for symbol in target_symbols if str(symbol or "").strip()}
    latest = (
        combined_local[combined_local["symbol"].astype(str).str.upper().isin(symbols)]
        .sort_values(["symbol", "date"], kind="stable")
        .groupby("symbol", sort=False)
        .tail(1)
        .copy()
    )
    target_dates = list(pd.to_datetime(latest["date"], errors="coerce").dropna().unique())
    cross_started = time.perf_counter()
    _progress(progress, "CROSS_SECTION_BUILD", target_symbols=len(symbols), target_dates=len(target_dates))
    cross, _counts = _add_target_cross_section(combined_local, target_dates)
    target = (
        cross[cross["symbol"].astype(str).str.upper().isin(symbols)]
        .sort_values(["symbol", "date"], kind="stable")
        .groupby("symbol", sort=False)
        .tail(1)
        .copy()
    )
    cross_ms = (time.perf_counter() - cross_started) * 1000.0

    _progress(progress, "MODEL_LOAD", target_rows=len(target))
    load_started = time.perf_counter()
    bundle, error = _load_bundle()
    model_load_ms = (time.perf_counter() - load_started) * 1000.0
    if bundle is None:
        return target, {"available": False, "error": str(error or "model_unavailable")}, {
            "cross_section_ms": cross_ms,
            "model_load_ms": model_load_ms,
            "model_score_ms": 0.0,
        }

    score_started = time.perf_counter()
    cross_mask = pd.Series(target.get("technical_signal_live"), index=target.index).fillna(False).astype(bool)
    cross_index = target.index[cross_mask]
    _progress(progress, "MODEL_SCORE", target_rows=len(target), cross_rows=len(cross_index))
    target["_live_sniper_p50"] = np.nan
    target["_live_sniper_p100"] = np.nan
    if len(cross_index):
        for model_key, output_column in (("r50", "_live_sniper_p50"), ("r100", "_live_sniper_p100")):
            spec = bundle["config"][model_key]
            features = list(spec["features"])
            matrix = target.loc[cross_index, features].replace([np.inf, -np.inf], np.nan)
            raw = np.asarray(bundle[model_key].predict(matrix, num_threads=2), dtype=float)
            target.loc[cross_index, output_column] = _platt_calibrate(
                raw,
                float(spec["platt_logit_coefficient"]),
                float(spec["platt_intercept"]),
            )
    model_score_ms = (time.perf_counter() - score_started) * 1000.0
    status = dict(model_status() or {})
    status.update({"available": True, "error": "", "score_scope": "A98_TARGET_ROWS_EXACT"})
    return target, status, {
        "cross_section_ms": cross_ms,
        "model_load_ms": model_load_ms,
        "model_score_ms": model_score_ms,
    }


def score_batch(
    market_df: Any,
    records_by_symbol: Mapping[str, Iterable[Mapping[str, Any]]],
    *,
    market_key: str,
    snapshot_key: str = "",
    progress: ProgressCallback = None,
    sealed: bool = False,
    score_stage: str = "",
    grid_role: str = "",
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Exact cached replacement for estimate_market_live_probability_batch."""
    from live_pulse_seal_engine import (
        _A95_STAGE_BIRTH,
        _a95_score_evidence,
        _iso,
        _missing_probability_from_scored_row,
        _probability_feature_vector_sha256,
        classify_probability_kind,
    )
    from normalized_gann import build_normalized_gann_grid

    score_started_at = _iso()
    effective_stage = str(score_stage or _A95_STAGE_BIRTH)
    effective_grid_role = str(grid_role or ("r153_live_market_gann20" if sealed else "r159_batch_live_market_gann20"))
    clean, results = _clean_records(records_by_symbol)
    if market_df is None or not hasattr(market_df, "empty") or market_df.empty:
        return ({str(key).upper(): {"available": False, "error": "market_snapshot_unavailable"} for key in records_by_symbol}, {})
    if not clean:
        return results, {}

    base_local, cache_meta = _GLOBAL_CACHE.base_local(
        market_df, market_key=market_key, snapshot_key=snapshot_key, progress=progress
    )
    targets = set(clean)
    base_without_targets = base_local[~base_local["symbol"].astype(str).str.upper().isin(targets)].copy()
    fresh_rows = [row for rows in clean.values() for row in rows]
    target_local = _build_local(pd.DataFrame(fresh_rows))
    combined_local = pd.concat([base_without_targets, target_local], ignore_index=True, sort=False)
    combined_local = combined_local.sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)

    target_rows, status, score_meta = _score_target_rows(combined_local, sorted(targets), progress=progress)
    total_symbols = int(combined_local["symbol"].astype(str).nunique()) if "symbol" in combined_local.columns else 0
    feature_names = list((status or {}).get("feature_names") or [])
    feature_score_ms = float(cache_meta.get("feature_cache_prepare_ms") or 0.0) + float(score_meta.get("cross_section_ms") or 0.0) + float(score_meta.get("model_load_ms") or 0.0) + float(score_meta.get("model_score_ms") or 0.0)

    for symbol in clean:
        target = target_rows[target_rows["symbol"].astype(str).str.upper() == symbol]
        if target.empty:
            results[symbol] = {"available": False, "error": "target_missing_after_score"}
            continue
        last = target.iloc[-1]
        p50_raw = _finite(last.get("_live_sniper_p50"))
        p100_raw = _finite(last.get("_live_sniper_p100"))
        p50 = p50_raw * 100.0 if math.isfinite(p50_raw) else float("nan")
        p100 = p100_raw * 100.0 if math.isfinite(p100_raw) else float("nan")
        anchor = _finite(last.get("close"))
        if not math.isfinite(p50):
            results[symbol] = _missing_probability_from_scored_row(
                last, feature_count_received=len(clean.get(symbol) or []), sealed=bool(sealed)
            )
            continue
        grid = build_normalized_gann_grid(
            anchor, market_key=str(market_key or ""), symbol=symbol, grid_role=effective_grid_role
        )
        bar_time = pd.to_datetime(last.get("date"), errors="coerce")
        target_count = int(
            combined_local[pd.to_datetime(combined_local["date"], errors="coerce") == bar_time]["symbol"].nunique()
        ) if not pd.isna(bar_time) else 0
        row_dict = last.to_dict()
        results[symbol] = {
            "available": True,
            "p50_pct": p50,
            "p100_pct": p100,
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
            "probability_feature_score_ms": round(feature_score_ms, 3),
            "feature_schema_sha256": str((status or {}).get("feature_schema_sha256") or ""),
            "feature_count_expected": int((status or {}).get("feature_count_expected") or 0),
            "model_sha256": str((status or {}).get("model_sha256") or ""),
            "model_r50_sha256": str((status or {}).get("r50_sha256") or ""),
            "model_r100_sha256": str((status or {}).get("r100_sha256") or ""),
            "probability_feature_vector_sha256": _probability_feature_vector_sha256(row_dict, feature_names),
            **_a95_score_evidence(
                row_dict, status, effective_stage, str(market_key or ""), combined_local,
                total_symbols, target_count, score_started_at,
            ),
        }
    return results, {**cache_meta, **score_meta, "feature_build_ms": round(float(cache_meta.get("feature_cache_prepare_ms") or 0.0) + float(score_meta.get("cross_section_ms") or 0.0), 3)}


def score_single(
    market_df: Any,
    records: Iterable[Mapping[str, Any]],
    *,
    market_key: str,
    symbol: str,
    sealed: bool = False,
    snapshot_key: str = "",
    progress: ProgressCallback = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    sym = str(symbol or "").strip().upper()
    from live_pulse_seal_engine import _a95_score_stage
    stage = _a95_score_stage(sealed=bool(sealed))
    batch, meta = score_batch(
        market_df,
        {sym: list(records or [])},
        market_key=market_key,
        snapshot_key=snapshot_key,
        progress=progress,
        sealed=bool(sealed),
        score_stage=stage,
        grid_role="r153_live_market_gann20",
    )
    return dict(batch.get(sym) or {"available": False, "error": "target_missing_after_score"}), meta


def full_pipeline_prewarm(progress: ProgressCallback = None) -> Dict[str, Any]:
    """Exercise imports, feature code and both models without polluting market caches."""
    _progress(progress, "PREWARM_IMPORTS")
    from gann20_probability_model import _load_bundle, model_status
    from radar30m_live_engine import _add_pulse_features, _add_symbol_features, _prepare_df

    rows: List[Dict[str, Any]] = []
    base_time = pd.Timestamp("2024-01-02 10:00:00")
    for symbol_index, symbol in enumerate(("A98W1", "A98W2", "A98W3")):
        for index in range(32):
            close = 100.0 + symbol_index * 3.0 + index * 0.05 + math.sin(index / 3.0) * 0.2
            rows.append({
                "date": base_time + pd.Timedelta(minutes=30 * index),
                "symbol": symbol,
                "open": close - 0.05,
                "high": close + 0.12,
                "low": close - 0.12,
                "close": close,
                "volume": 1000.0 + index * 10.0,
                "name": symbol,
            })
    _progress(progress, "PREWARM_FEATURES")
    local = _prepare_df(pd.DataFrame(rows))
    local = _add_symbol_features(local)
    local = _add_pulse_features(local)
    local = _add_model_local_features(local)
    cross, _ = _add_target_cross_section(local, [local["date"].max()])
    _progress(progress, "PREWARM_MODEL")
    bundle, error = _load_bundle()
    status = dict(model_status() or {})
    if bundle is None:
        return {"ok": False, "error": str(error or "model_unavailable"), "model_status": status}
    # Run predict on one row even if the synthetic row is not a real cross.
    row = cross.tail(1).copy()
    from gann20_probability_model import _platt_calibrate
    outputs: Dict[str, float] = {}
    for model_key in ("r50", "r100"):
        spec = bundle["config"][model_key]
        raw = np.asarray(bundle[model_key].predict(row.loc[:, list(spec["features"])], num_threads=1), dtype=float)
        outputs[model_key] = float(_platt_calibrate(raw, float(spec["platt_logit_coefficient"]), float(spec["platt_intercept"]))[0])
    return {
        "ok": True,
        "pipeline_prewarmed": True,
        "contract_version": CACHE_CONTRACT_VERSION,
        "model_status": status,
        "synthetic_scores": outputs,
    }


__all__ = [
    "CACHE_CONTRACT_VERSION",
    "ProbabilityMarketFeatureCache",
    "cache_snapshot",
    "clear_cache",
    "score_batch",
    "score_single",
    "full_pipeline_prewarm",
]


def _timing_ms(value: Any) -> float | None:
    number = _finite(value)
    return round(number, 3) if math.isfinite(number) and number >= 0.0 else None


def probability_hot_path_timing_fields(meta: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize A98 worker/cache timing fields for lifecycle logs."""
    return {
        "probability_hot_path": bool(meta.get("probability_hot_path")),
        "probability_hot_path_fallback": bool(meta.get("probability_hot_path_fallback")),
        "feature_cache_hit": bool(meta.get("feature_cache_hit")),
        "feature_cache_exact_snapshot_hit": bool(meta.get("feature_cache_exact_snapshot_hit")),
        "feature_cache_reused_symbols": meta.get("feature_cache_reused_symbols"),
        "feature_cache_rebuilt_symbols": meta.get("feature_cache_rebuilt_symbols"),
        "feature_cache_prepare_ms": _timing_ms(meta.get("feature_cache_prepare_ms")),
        "cross_section_ms": _timing_ms(meta.get("cross_section_ms")),
        "probability_progress_grace_used": bool(meta.get("probability_progress_grace_used")),
        "probability_progress_grace_sec": _timing_ms(float(meta.get("probability_progress_grace_sec") or 0.0) * 1000.0),
        "probability_progress_stage": str(meta.get("progress_stage") or ""),
        "probability_progress_stage_seq": meta.get("progress_stage_seq"),
    }
