                       
                              
                                                             

from __future__ import annotations
from exception_observability import report_suppressed_exception as _report_suppressed_exception

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple
import math
import datetime as _dt

try:
    from dashboard_session_snapshot import aggregate_pulse_map
except Exception:
    aggregate_pulse_map = None

try:
    import pandas as pd
except Exception:                    
    pd = None


def _to_float(x: Any, default: float = float('nan')) -> float:
    try:
        if x is None:
            return default
        v = float(x)
        if math.isfinite(v):
            return v
        return default
    except Exception:
        return default


def _to_dt(x: Any) -> Optional[_dt.datetime]:
    if x is None:
        return None
    if isinstance(x, _dt.datetime):
        return x.replace(tzinfo=None)
    if isinstance(x, _dt.date):
        return _dt.datetime.combine(x, _dt.time())
    try:
        if pd is not None:
            t = pd.to_datetime(x, errors="coerce")
            if pd.isna(t):
                return None
            return t.to_pydatetime().replace(tzinfo=None)
    except Exception as _suppressed_exc:
        _report_suppressed_exception(
            _suppressed_exc, module=__name__, file=__file__,
            function='_to_dt', line=49,
            stage='', critical=False,
        )
    try:
        return _dt.datetime.fromisoformat(str(x).replace("Z", ""))
    except Exception:
        return None


def _wma_last(values: List[float], length: int) -> float:
    vals = [v for v in values[-length:] if math.isfinite(v)]
    if not vals:
        return float('nan')
    n = len(vals)
    weights = list(range(1, n + 1))
    den = n * (n + 1) / 2.0
    return sum(v * w for v, w in zip(vals, weights)) / den


def _rsi_wilder_series(closes: List[float], length: int = 4) -> List[float]:
    """Small-window Wilder RSI computed on the in-memory deque only.

    This is O(50) per changed symbol, not O(full-history). It intentionally
    mirrors the live radar's RSIScaled=4 identity without using pandas rolling.
    """
    if not closes:
        return []
    out = [50.0]
    avg_gain = 0.0
    avg_loss = 0.0
    alpha = 1.0 / max(int(length), 1)
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = max(d, 0.0)
        loss = max(-d, 0.0)
        if i == 1:
            avg_gain = gain
            avg_loss = loss
        else:
            avg_gain = (1 - alpha) * avg_gain + alpha * gain
            avg_loss = (1 - alpha) * avg_loss + alpha * loss
        if avg_loss == 0:
            rsi = 100.0 if avg_gain > 0 else 50.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))
        out.append(max(0.0, min(100.0, float(rsi))))
    return out


@dataclass
class _SymbolBars:
    bars: deque = field(default_factory=lambda: deque(maxlen=60))
    last_dt: Optional[_dt.datetime] = None


class StatefulPulseTape:
    """In-memory pulse state for RSIScaled(4) / VAR3(11).

    The engine stores only the last max_bars candles per symbol. Updating a new
    bar changes only that symbol's deque; computing the latest state is O(max_bars)
    for changed symbols. This replaces replay/live full-history pulse recompute.
    """

    def __init__(self, max_bars: int = 150):
                                                                            
                                                                             
        self.max_bars = int(max(150, max_bars))
        self._symbols: Dict[str, _SymbolBars] = {}
        self._last_map: Dict[str, Dict[str, Any]] = {}

    def reset(self) -> None:
        self._symbols.clear()
        self._last_map.clear()

    def update_from_rows(self, rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        changed: List[str] = []
        for row in rows or []:
            sym = str(row.get("symbol", "") or "").strip().upper()
            if not sym:
                continue
            dt = _to_dt(row.get("date") or row.get("datetime") or row.get("data_datetime") or row.get("last_datetime"))
            if dt is None:
                continue
            close = _to_float(row.get("close", row.get("current_price", row.get("price"))))
            high = _to_float(row.get("high", close))
            low = _to_float(row.get("low", close))
            open_ = _to_float(row.get("open", close))
            volume = _to_float(row.get("volume", 0.0), 0.0)
            if not (math.isfinite(close) and close > 0 and math.isfinite(high) and math.isfinite(low)):
                continue
            slot = self._symbols.setdefault(sym, _SymbolBars(deque(maxlen=self.max_bars)))
            bar = {
                "symbol": sym, "date": dt, "open": open_, "high": high,
                "low": low, "close": close, "volume": volume,
                "name": row.get("name") or row.get("company_name") or sym,
            }
            if slot.bars and slot.bars[-1].get("date") == dt:
                slot.bars[-1] = bar                                               
            elif slot.last_dt is None or dt > slot.last_dt:
                slot.bars.append(bar)
            else:
                                                                                                         
                if all(b.get("date") != dt for b in slot.bars):
                    tmp = list(slot.bars) + [bar]
                    tmp.sort(key=lambda b: b.get("date"))
                    slot.bars.clear()
                    for b in tmp[-self.max_bars:]:
                        slot.bars.append(b)
                else:
                    continue
            slot.last_dt = slot.bars[-1].get("date") if slot.bars else dt
            changed.append(sym)
        for sym in set(changed):
            info = self._compute_symbol(sym)
            if info:
                self._last_map[sym] = info
        return {s: self._last_map[s] for s in set(changed) if s in self._last_map}

    def update_from_dataframe(self, df: Any, tail_per_symbol: int = 80) -> Dict[str, Dict[str, Any]]:
        if df is None:
            return {}
        if pd is None:
            return {}
        try:
            work = df.copy()
            if "symbol" not in work.columns:
                return {}
            work["symbol"] = work["symbol"].astype(str).str.strip().str.upper()
            if "date" in work.columns:
                work["date"] = pd.to_datetime(work["date"], errors="coerce")
            else:
                return {}
            work = work.dropna(subset=["symbol", "date", "close"]).sort_values(["symbol", "date"])
            if tail_per_symbol:
                work = work.groupby("symbol", as_index=False, sort=False).tail(int(tail_per_symbol))
            return self.update_from_rows(work.to_dict("records"))
        except Exception:
            return {}

    def _compute_symbol(self, sym: str) -> Optional[Dict[str, Any]]:
        slot = self._symbols.get(sym)
        if not slot or len(slot.bars) < 6:
            return None
        bars = list(slot.bars)
        closes = [_to_float(b.get("close")) for b in bars]
        highs = [_to_float(b.get("high")) for b in bars]
        lows = [_to_float(b.get("low")) for b in bars]
        dates = [b.get("date") for b in bars]
        rsi = _rsi_wilder_series(closes, 4)
        rsiscales: List[float] = []
        var3s: List[float] = []
        gaps: List[float] = []
        var2 = 0
        for i in range(len(bars)):
            lo = min(closes[max(0, i - 3): i + 1])
            hi = max(closes[max(0, i - 3): i + 1])
            rs = lo + (rsi[i] / 100.0) * (hi - lo)
            rsiscales.append(rs)
            wh_prev = _wma_last(highs[:i], 11) if i > 0 else float('nan')
            wl_prev = _wma_last(lows[:i], 11) if i > 0 else float('nan')
            if math.isfinite(wh_prev) and rs > wh_prev:
                var1 = 1
            elif math.isfinite(wl_prev) and rs < wl_prev:
                var1 = -1
            else:
                var1 = 0
            if var1 != 0:
                var2 = var1
            wh = _wma_last(highs[:i + 1], 11)
            wl = _wma_last(lows[:i + 1], 11)
            v3 = wh if var2 == -1 else wl
            var3s.append(v3)
            gaps.append(rs - v3 if math.isfinite(v3) else float('nan'))
        i = len(bars) - 1
        close = closes[i]
        gap = gaps[i]
        gap_pct = (gap / close * 100.0) if math.isfinite(gap) and close else float('nan')
        active = bool(math.isfinite(gap) and gap >= 0)
                                                                           
                                                                                    
        def _date_key(x):
            if isinstance(x, _dt.datetime):
                return x.date()
            try:
                dt0 = _to_dt(x)
                return dt0.date() if dt0 else None
            except Exception:
                return None
        latest_session_key = _date_key(dates[i])
        if latest_session_key is not None:
            session_positions = [k for k, d in enumerate(dates) if _date_key(d) == latest_session_key]
        else:
            session_positions = list(range(len(bars)))
        # V86M: لا نحسب العمر من جلسة اليوم فقط.  العمر من آخر تقاطع فعلي مرصود
        # داخل الذاكرة الحالية، سواء حدث اليوم أو في جلسة سابقة.
        cross_up_positions = []
        cross_down_positions = []
        for j in range(len(bars)):
            if j <= 0:
                continue
            prev_ok = math.isfinite(rsiscales[j - 1]) and math.isfinite(var3s[j - 1])
            now_ok = math.isfinite(rsiscales[j]) and math.isfinite(var3s[j])
            if not (prev_ok and now_ok):
                continue
            if rsiscales[j - 1] < var3s[j - 1] and rsiscales[j] >= var3s[j]:
                cross_up_positions.append(j)
            if rsiscales[j - 1] >= var3s[j - 1] and rsiscales[j] < var3s[j]:
                cross_down_positions.append(j)
        last_cross = cross_up_positions[-1] if cross_up_positions else None
        last_down = cross_down_positions[-1] if cross_down_positions else None
        positive_age = (i - last_cross) if (active and last_cross is not None and (last_down is None or last_cross > last_down)) else None
        negative_age = (i - last_down) if ((not active) and last_down is not None and (last_cross is None or last_down > last_cross)) else None
        age = positive_age if active else negative_age
        failed = bool((not active) and negative_age is not None)
        gap_prev = gaps[i - 1] if i >= 1 else float('nan')
        rsi_slope = rsiscales[i] - rsiscales[i - 1] if i >= 1 else 0.0
        gap_improving = bool(math.isfinite(gap_prev) and math.isfinite(gap) and gap > gap_prev)
        near = bool((not active) and (not failed) and gap_improving and rsi_slope > 0 and math.isfinite(gap_pct) and gap_pct > -0.75)
        if failed:
            state_code, state_ar, score = "failed", "فشل نبضة", 15.0
        elif active and age is not None and age <= 2:
            state_code, state_ar, score = "new", "نبضة جديدة", 95.0
        elif active:
            extended = bool((age is not None and age >= 7) or (math.isfinite(gap_pct) and gap_pct >= 1.25))
            if extended:
                state_code, state_ar, score = "extended", "ممتد — لا تطارد", 55.0
            else:
                state_code, state_ar, score = "active", "نبضة نشطة", 78.0
        elif near:
            state_code, state_ar, score = "near", "قريب من نبضة", 62.0
        else:
            state_code, state_ar, score = "none", "بدون نبضة", 30.0
        last_pos_dt = dates[last_cross] if last_cross is not None else None
        last_neg_dt = dates[last_down] if last_down is not None else None
        pos_cross_session = _date_key(last_pos_dt).isoformat() if _date_key(last_pos_dt) is not None else ""
        neg_cross_session = _date_key(last_neg_dt).isoformat() if _date_key(last_neg_dt) is not None else ""
        new_this_session = bool(active and latest_session_key is not None and pos_cross_session == latest_session_key.isoformat())
        failed_this_session = bool((not active) and latest_session_key is not None and neg_cross_session == latest_session_key.isoformat())
        new_this_bar = bool(active and last_cross is not None and last_cross == i)
        failed_this_bar = bool((not active) and last_down is not None and last_down == i)

        last_dt = dates[i]
        if isinstance(last_dt, _dt.datetime):
            dt_text = last_dt.strftime("%Y-%m-%d %H:%M:%S")
            time_text = last_dt.strftime("%H:%M:%S")
            sess = last_dt.date().isoformat()
        else:
            dt_text = str(last_dt or "")
            time_text = dt_text[-8:] if len(dt_text) >= 8 else dt_text
            sess = dt_text[:10]
        return {
            "symbol": sym,
            "pulse_state": state_code,
            "pulse_state_ar": state_ar,
            "pulse_score": float(score),
            "pulse_age_bars": age,
            "positive_pulse_age_bars": positive_age,
            "negative_pulse_age_bars": negative_age,
            "pulse_polarity": "positive" if active else "negative",
            "pulse_is_above": bool(active),
            "positive_cross_datetime": (last_pos_dt.strftime("%Y-%m-%d %H:%M:%S") if isinstance(last_pos_dt, _dt.datetime) else (str(last_pos_dt) if last_pos_dt is not None else "")),
            "negative_cross_datetime": (last_neg_dt.strftime("%Y-%m-%d %H:%M:%S") if isinstance(last_neg_dt, _dt.datetime) else (str(last_neg_dt) if last_neg_dt is not None else "")),
            "positive_cross_session_date": pos_cross_session,
            "negative_cross_session_date": neg_cross_session,
            "pulse_new_this_session": bool(new_this_session),
            "pulse_failed_this_session": bool(failed_this_session),
            "pulse_new_this_bar": bool(new_this_bar),
            "pulse_failed_this_bar": bool(failed_this_bar),
            "pulse_gap_pct": float(gap_pct) if math.isfinite(gap_pct) else float('nan'),
            "rsiscaled": float(rsiscales[i]) if math.isfinite(rsiscales[i]) else float('nan'),
            "var3": float(var3s[i]) if math.isfinite(var3s[i]) else float('nan'),
            "pulse_last_time": time_text,
            "pulse_last_datetime": dt_text,
            "pulse_cross_datetime": (dates[last_cross].strftime("%Y-%m-%d %H:%M:%S") if (active and last_cross is not None and isinstance(dates[last_cross], _dt.datetime)) else (str(dates[last_cross]) if active and last_cross is not None else (dates[last_down].strftime("%Y-%m-%d %H:%M:%S") if ((not active) and last_down is not None and isinstance(dates[last_down], _dt.datetime)) else (str(dates[last_down]) if ((not active) and last_down is not None) else "")))),
            "session_date": sess,
        }

    def pulse_map(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._last_map)

    def snapshot(self, exclude_symbols: Optional[Iterable[str]] = None) -> Dict[str, Any]:
        exclude = {str(x).strip().upper() for x in (exclude_symbols or [])}
        pulse_map = {str(k).upper(): dict(v or {}) for k, v in self._last_map.items() if str(k).upper() not in exclude}
        if not pulse_map:
            return {"pulse_universe_count": 0, "pulse_text": "-", "pulse_label": "-"}
        if aggregate_pulse_map is not None:
            agg = aggregate_pulse_map(pulse_map)
        else:
            rows = list(pulse_map.values())
            total = len(rows)
            active_count = sum(1 for r in rows if str(r.get("pulse_polarity") or "") == "positive")
            agg = {
                "pulse_universe_count": total,
                "pulse_active_count": active_count,
                "pulse_active_pct": active_count / total * 100.0 if total else 0.0,
                "pulse_new_pct": sum(1 for r in rows if r.get("pulse_state") == "new") / total * 100.0 if total else 0.0,
                "pulse_failed_pct": sum(1 for r in rows if r.get("pulse_state") == "failed") / total * 100.0 if total else 0.0,
                "pulse_near_pct": sum(1 for r in rows if r.get("pulse_state") == "near") / total * 100.0 if total else 0.0,
                "pulse_no_pct": sum(1 for r in rows if r.get("pulse_state") == "none") / total * 100.0 if total else 0.0,
                "pulse_session_date": max([str(r.get("session_date") or "") for r in rows] or [""]),
            }
        active_pct = float(agg.get("pulse_active_pct") or 0.0)
        new_pct = float(agg.get("pulse_new_pct") or 0.0)
        failed_pct = float(agg.get("pulse_failed_pct") or 0.0)
        near_pct = float(agg.get("pulse_near_pct") or 0.0)
        if failed_pct >= max(new_pct + 6.0, 8.0):
            label = "تحذير فشل نبضات"
        elif active_pct >= 60.0 and new_pct < 4.0:
            label = "قوي لكن ممتد"
        elif active_pct >= 52.0 and new_pct >= 5.0:
            label = "نبض قوي"
        elif active_pct >= 38.0 or new_pct >= 8.0:
            label = "إيجابي"
        elif near_pct >= 18.0 and new_pct >= 2.0:
            label = "بداية تحسن"
        elif active_pct >= 25.0:
            label = "محايد"
        else:
            label = "ضعيف"
        total = int(agg.get("pulse_universe_count") or 0)
        new_session = float(agg.get("pulse_new_session_pct") or 0.0)
        failed_session = float(agg.get("pulse_failed_session_pct") or 0.0)
        out = dict(agg)
        out.update({
            "pulse_label": label,
            "pulse_session_date": str(agg.get("pulse_session_date") or agg.get("latest_session_date") or ""),
            "latest_session_date": str(agg.get("pulse_session_date") or agg.get("latest_session_date") or ""),
            "pulse_text": f"{label}: فوق {active_pct:.1f}%، جديد جلسة {new_session:.1f}%، فشل جلسة {failed_session:.1f}%",
            "pulse_counts_text": f"عدد={total} | فوق={int(agg.get('pulse_active_count') or 0)} | جديد جلسة={int(agg.get('pulse_new_session_count') or 0)} | فشل جلسة={int(agg.get('pulse_failed_session_count') or 0)}",
        })
        return out
