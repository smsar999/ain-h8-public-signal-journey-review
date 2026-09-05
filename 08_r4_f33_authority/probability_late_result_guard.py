# -*- coding: utf-8 -*-
"""Execution-time price guard for probability results that finish after the cross.

The model must keep the immutable birth observation as its input.  This module
therefore never changes the score anchor.  It only attaches a separately named
latest quote so the lifecycle engine can decide whether R50/stop was already
consumed before a delayed result is surfaced.
"""
from __future__ import annotations

import datetime as dt
import math
from typing import Any, Dict, Mapping, Tuple


def _finite(value: Any) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else float("nan")
    except (TypeError, ValueError, OverflowError):
        return float("nan")


def latest_symbol_quote(frame: Any, symbol: str) -> Dict[str, Any]:
    if frame is None or not hasattr(frame, "columns") or getattr(frame, "empty", True):
        return {}
    try:
        import pandas as pd
        cols = {str(column).lower(): column for column in frame.columns}
        symbol_col = cols.get("symbol")
        if symbol_col is None:
            return {}
        sym = str(symbol or "").strip().upper()
        rows = frame[frame[symbol_col].astype(str).str.strip().str.upper() == sym]
        if rows.empty:
            return {}
        date_col = cols.get("date") or cols.get("datetime") or cols.get("time")
        if date_col is not None:
            ordered = rows.assign(_a98_quote_time=pd.to_datetime(rows[date_col], errors="coerce")).sort_values("_a98_quote_time")
            row = ordered.iloc[-1]
            quote_time = row.get("_a98_quote_time")
        else:
            row = rows.iloc[-1]
            quote_time = None
        close_col = cols.get("close") or cols.get("price") or cols.get("last")
        high_col = cols.get("high")
        low_col = cols.get("low")
        price = _finite(row.get(close_col)) if close_col is not None else float("nan")
        high = _finite(row.get(high_col)) if high_col is not None else price
        low = _finite(row.get(low_col)) if low_col is not None else price
        if not math.isfinite(price) or price <= 0.0:
            return {}
        return {
            "price": price,
            "high": high if math.isfinite(high) else price,
            "low": low if math.isfinite(low) else price,
            "bar_time": "" if quote_time is None or pd.isna(quote_time) else str(quote_time),
        }
    except Exception:
        return {}


def _attach_resolved_quote(
    observation: Mapping[str, Any], probability: Mapping[str, Any], quote: Mapping[str, Any] | None, *, source: str,
    market_key: str = "",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    obs = dict(observation or {})
    prob = dict(probability or {})
    resolved = dict(quote or {})
    checked_at = dt.datetime.now(dt.timezone.utc).isoformat()
    if resolved:
        try:
            import pandas as pd
            quote_time = pd.to_datetime(resolved.get("bar_time"), errors="coerce", utc=True)
            observation_utc = obs.get("episode_signal_bar_end_utc")
            if observation_utc:
                observation_time = pd.to_datetime(observation_utc, errors="coerce", utc=True)
            else:
                raw_observation_time = (
                    obs.get("canonical_market_bar_time") or obs.get("episode_signal_bar_time")
                    or obs.get("bar_datetime") or obs.get("date") or obs.get("time")
                )
                observation_time = pd.to_datetime(raw_observation_time, errors="coerce")
                if pd.notna(observation_time) and getattr(observation_time, "tzinfo", None) is None and market_key:
                    from market_datetime_normalizer import strict_market_timezone_name
                    observation_time = observation_time.tz_localize(strict_market_timezone_name(market_key))
                if pd.notna(observation_time):
                    observation_time = observation_time.tz_convert("UTC") if getattr(observation_time, "tzinfo", None) is not None else observation_time
            if pd.notna(quote_time) and pd.notna(observation_time):
                if getattr(observation_time, "tzinfo", None) is None:
                    quote_cmp = quote_time.tz_localize(None)
                else:
                    quote_cmp = quote_time
                if quote_cmp < observation_time:
                    resolved = {}
        except Exception as exc:
            resolved = {}
            prob.update({
                "probability_result_time_compare_error_type": type(exc).__name__,
                "probability_result_time_compare_error_message": str(exc),
            })
    if not resolved:
        prob.update({
            "probability_result_price_checked_at": checked_at,
            "probability_result_price_available": False,
        })
        return obs, prob
    obs.update({
        "probability_result_current_price": resolved["price"],
        "probability_result_current_high": resolved["high"],
        "probability_result_current_low": resolved["low"],
        "probability_result_current_bar_time": resolved.get("bar_time") or "",
        "probability_result_price_checked_at": checked_at,
        "probability_result_guard_price_only": True,
    })
    prob.update({
        "probability_result_current_price": resolved["price"],
        "probability_result_current_high": resolved["high"],
        "probability_result_current_low": resolved["low"],
        "probability_result_current_bar_time": resolved.get("bar_time") or "",
        "probability_result_price_checked_at": checked_at,
        "probability_result_price_available": True,
        "probability_result_price_source": str(source or "LATEST_EXECUTION_GUARD"),
        "probability_result_guard_price_only": True,
    })
    return obs, prob


def _authority_quote(owner: Any, market_key: str, symbol: str) -> Dict[str, Any]:
    store = getattr(owner, "_live_price_authority_store", None)
    if store is None or not hasattr(store, "get"):
        return {}
    selected = store.get(str(market_key), str(symbol), purpose="current_price")
    if not isinstance(selected, Mapping) or not selected:
        return {}
    price = _finite(selected.get("current_price", selected.get("price", selected.get("close"))))
    if not math.isfinite(price) or price <= 0.0:
        return {}
    high = _finite(selected.get("high")); low = _finite(selected.get("low"))
    return {
        "price": price,
        "high": high if math.isfinite(high) else price,
        "low": low if math.isfinite(low) else price,
        "bar_time": str(
            selected.get("market_event_time") or selected.get("bar_end_utc")
            or selected.get("data_datetime") or selected.get("current_price_time") or ""
        ),
    }


def attach_latest_quote(
    observation: Mapping[str, Any], probability: Mapping[str, Any], frame: Any, *, symbol: str
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    return _attach_resolved_quote(
        observation, probability, latest_symbol_quote(frame, symbol),
        source="LATEST_MARKET_CACHE_EXECUTION_GUARD",
    )


def attach_latest_quote_for_owner(
    owner: Any, market_key: str, observation: Mapping[str, Any], probability: Mapping[str, Any],
    *, symbol: str, on_error: Any = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Resolve the durable current-price authority first, then market caches."""
    try:
        authority = _authority_quote(owner, str(market_key), str(symbol))
        if authority:
            return _attach_resolved_quote(
                observation, probability, authority,
                source="DURABLE_CURRENT_PRICE_AUTHORITY_EXECUTION_GUARD", market_key=str(market_key),
            )
        live_cache = getattr(owner, "_last_live_cache_df_by_market", {}) or {}
        model_cache = getattr(owner, "_last_model_snapshot_df_by_market", {}) or {}
        frame = live_cache.get(str(market_key))
        if frame is None or getattr(frame, "empty", True):
            frame = model_cache.get(str(market_key))
        return attach_latest_quote(observation, probability, frame, symbol=symbol)
    except Exception as exc:
        if callable(on_error):
            on_error(
                "probability_attach", "attach_probability_latest_price_guard", exc,
                market=str(market_key), symbol=str(symbol),
                source_observation_id=str(dict(observation or {}).get("source_observation_id") or ""),
                reason_code="PROBABILITY_LATE_PRICE_GUARD_FAILED",
            )
        prob = dict(probability or {})
        prob.update({
            "probability_result_price_available": False,
            "probability_result_price_guard_error_type": type(exc).__name__,
            "probability_result_price_guard_error_message": str(exc),
        })
        return dict(observation or {}), prob


__all__ = ["latest_symbol_quote", "attach_latest_quote", "attach_latest_quote_for_owner"]
