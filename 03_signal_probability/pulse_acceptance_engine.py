# -*- coding: utf-8 -*-
"""V86BD — deterministic intrabar pulse acceptance engine.

This module is intentionally independent from Qt, the heavy market-wide radar,
and the official lifecycle ledger.  It consumes the already-read tail of one
symbol and answers a narrow technical question:

    Did RSIScaled cross VAR3 now, and is the forming bar holding that cross?

Truth contract
--------------
* ``LIVE_*`` rows are intrabar observations, never sealed recommendations.
* ``first_cross_at`` is the first time this process *observed* the cross.  It is
  not presented as an exchange tick timestamp when the source is file-polled.
* ``first_cross_price`` is immutable for the episode.
* Fast confirmation uses hysteresis and at least two distinct observations.
* Market, decision lane, symbol and session are isolated in the state key.
* Official/sealed promotion remains the responsibility of the heavy radar.

The pulse formula mirrors ``radar30m_live_engine._add_pulse_features``:
RSI(4) -> RSIScaled(4) -> WMA high/low(11) -> VAR3.
"""
from __future__ import annotations
from exception_observability import report_suppressed_exception as _report_suppressed_exception

from dataclasses import asdict, dataclass, field
import datetime as _dt
import math
import threading
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


LIVE_CROSS = "LIVE_CROSS"
LIVE_ARMED_FAST = "LIVE_ARMED_FAST"
LIVE_ACCEPTED = "LIVE_ACCEPTED"
LIVE_TREND_CONFIRMED = "LIVE_TREND_CONFIRMED"
LIVE_WEAKENING = "LIVE_WEAKENING"
LIVE_FAILED = "LIVE_FAILED"
LIVE_LATE_NO_CHASE = "LIVE_LATE_NO_CHASE"
LIVE_EXPIRED = "LIVE_EXPIRED"
SEALED_BY_RADAR = "SEALED_BY_RADAR"

_TERMINAL_STATES = {LIVE_FAILED, LIVE_EXPIRED}
PUBLISHABLE_FAST_PULSE_STATES = {LIVE_CROSS, LIVE_ARMED_FAST, LIVE_ACCEPTED}


def is_publishable_fast_pulse_state(state: Any) -> bool:
    """The trader-facing fast lane is only cross moment + quick confirmation."""
    return str(state or "") in PUBLISHABLE_FAST_PULSE_STATES


def _finite(value: Any, default: float = float("nan")) -> float:
    try:
        out = float(value)
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _iso(value: Any) -> str:
    if isinstance(value, _dt.datetime):
        return value.replace(tzinfo=None).isoformat(sep=" ", timespec="milliseconds")
    if isinstance(value, _dt.date):
        return value.isoformat()
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        # Accept pandas Timestamp without importing pandas.
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime().replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
        return _dt.datetime.fromisoformat(text.replace("T", " ")).replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    except Exception:
        return text


def _parse_dt(value: Any) -> Optional[_dt.datetime]:
    if isinstance(value, _dt.datetime):
        return value.replace(tzinfo=None)
    try:
        if hasattr(value, "to_pydatetime"):
            return value.to_pydatetime().replace(tzinfo=None)
    except Exception as _suppressed_exc:
        _report_suppressed_exception(
            _suppressed_exc, module=__name__, file=__file__,
            function='_parse_dt', line=82,
            stage='', critical=False,
        )
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return _dt.datetime.fromisoformat(text.replace("T", " ")).replace(tzinfo=None)
    except Exception:
        return None


def _bar_age_bars(anchor_bar: Any, current_bar: Any, timeframe_minutes: int = 30) -> int:
    start = _parse_dt(anchor_bar)
    end = _parse_dt(current_bar)
    if not start or not end or end < start:
        return 0
    seconds = max(1, int(timeframe_minutes or 30)) * 60
    return int((end - start).total_seconds() // seconds)


def _ewm_wilder(values: Sequence[float], alpha: float) -> List[float]:
    """Pandas adjust=False/min_periods=1 behaviour for a series beginning NaN."""
    out: List[float] = []
    prev: Optional[float] = None
    for value in values:
        if not math.isfinite(value):
            out.append(float("nan") if prev is None else float(prev))
            continue
        if prev is None:
            prev = float(value)
        else:
            prev = float(alpha) * float(value) + (1.0 - float(alpha)) * float(prev)
        out.append(float(prev))
    return out


def _rolling_wma(values: Sequence[float], window: int) -> List[float]:
    window = max(1, int(window))
    full_weights = [float(i) for i in range(1, window + 1)]
    out: List[float] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        chunk = list(values[start : i + 1])
        weights = full_weights[-len(chunk) :]
        valid = [(v, w) for v, w in zip(chunk, weights) if math.isfinite(v)]
        denom = sum(w for _, w in valid)
        out.append(sum(v * w for v, w in valid) / denom if denom > 0 else float("nan"))
    return out


def _rolling_max(values: Sequence[float], window: int, index: int) -> float:
    start = max(0, index - max(1, int(window)) + 1)
    valid = [float(v) for v in values[start : index + 1] if math.isfinite(v)]
    return max(valid) if valid else float("nan")


def _rolling_min(values: Sequence[float], window: int, index: int) -> float:
    start = max(0, index - max(1, int(window)) + 1)
    valid = [float(v) for v in values[start : index + 1] if math.isfinite(v)]
    return min(valid) if valid else float("nan")


def _mean(values: Sequence[float]) -> float:
    valid = [float(v) for v in values if math.isfinite(v)]
    return sum(valid) / len(valid) if valid else float("nan")


def _atr(high: Sequence[float], low: Sequence[float], close: Sequence[float], window: int = 14) -> float:
    trs: List[float] = []
    for i in range(len(close)):
        if not (math.isfinite(high[i]) and math.isfinite(low[i])):
            continue
        if i == 0 or not math.isfinite(close[i - 1]):
            tr = high[i] - low[i]
        else:
            tr = max(high[i] - low[i], abs(high[i] - close[i - 1]), abs(low[i] - close[i - 1]))
        if math.isfinite(tr):
            trs.append(float(max(0.0, tr)))
    return _mean(trs[-max(1, int(window)) :])


def _normalize_records(records: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for raw in records or []:
        r = dict(raw or {})
        dt = _parse_dt(r.get("date") or r.get("datetime") or r.get("time"))
        o = _finite(r.get("open"))
        h = _finite(r.get("high"))
        l = _finite(r.get("low"))
        c = _finite(r.get("close"))
        v = _finite(r.get("volume"), 0.0)
        if dt is None or not all(math.isfinite(x) and x > 0 for x in (o, h, l, c)):
            continue
        # Protect the technical calculation from malformed OHLC tails.
        h = max(h, o, l, c)
        l = min(l, o, h, c)
        normalized.append({"date": dt, "open": o, "high": h, "low": l, "close": c, "volume": max(0.0, v)})
    normalized.sort(key=lambda x: x["date"])
    return normalized


def compute_pulse_observation(
    records: Iterable[Mapping[str, Any]],
    *,
    market_key: str,
    symbol: str,
    name: str = "",
    observed_at: Any = None,
) -> Optional[Dict[str, Any]]:
    """Compute one compact, deterministic observation from an already-read tail.

    The function performs no I/O.  Its output is small enough to pass from the
    Price Tape reader to the state machine without retaining all market tails.
    """
    recs = _normalize_records(records)
    if len(recs) < 3:
        return None

    closes = [r["close"] for r in recs]
    highs = [r["high"] for r in recs]
    lows = [r["low"] for r in recs]
    opens = [r["open"] for r in recs]
    volumes = [r["volume"] for r in recs]

    deltas = [float("nan")]
    for i in range(1, len(closes)):
        deltas.append(closes[i] - closes[i - 1])
    ups = [float("nan") if not math.isfinite(d) else max(0.0, d) for d in deltas]
    downs = [float("nan") if not math.isfinite(d) else max(0.0, -d) for d in deltas]
    gain = _ewm_wilder(ups, 0.25)
    loss = _ewm_wilder(downs, 0.25)

    rsi: List[float] = []
    for g, loss_i in zip(gain, loss):
        if math.isfinite(g) and math.isfinite(loss_i) and loss_i > 0:
            rs = g / loss_i
            value = 100.0 - (100.0 / (1.0 + rs))
        else:
            value = 100.0 if math.isfinite(g) and g > 0 else 50.0
        rsi.append(min(100.0, max(0.0, float(value))))

    rsiscaled: List[float] = []
    for i, value in enumerate(rsi):
        hi = _rolling_max(closes, 4, i)
        lo = _rolling_min(closes, 4, i)
        rsiscaled.append((value / 100.0) * (hi - lo) + lo if math.isfinite(hi) and math.isfinite(lo) else float("nan"))

    wma_high = _rolling_wma(highs, 11)
    wma_low = _rolling_wma(lows, 11)
    var2: List[int] = []
    current_var2 = 0
    var3: List[float] = []
    for i, rs in enumerate(rsiscaled):
        prev_wh = wma_high[i - 1] if i > 0 else float("nan")
        prev_wl = wma_low[i - 1] if i > 0 else float("nan")
        if math.isfinite(rs) and math.isfinite(prev_wh) and rs > prev_wh:
            current_var2 = 1
        elif math.isfinite(rs) and math.isfinite(prev_wl) and rs < prev_wl:
            current_var2 = -1
        var2.append(current_var2)
        var3.append(wma_high[i] if current_var2 == -1 else wma_low[i])

    gaps = [rs - v3 if math.isfinite(rs) and math.isfinite(v3) else float("nan") for rs, v3 in zip(rsiscaled, var3)]
    gap_pcts = [g / c if math.isfinite(g) and c else float("nan") for g, c in zip(gaps, closes)]
    positive_crosses: List[bool] = [False] * len(recs)
    negative_crosses: List[bool] = [False] * len(recs)
    for i in range(1, len(recs)):
        if math.isfinite(gaps[i - 1]) and math.isfinite(gaps[i]):
            positive_crosses[i] = bool(gaps[i - 1] < 0 <= gaps[i])
            negative_crosses[i] = bool(gaps[i - 1] >= 0 > gaps[i])

    last_pos_idx = max((i for i, flag in enumerate(positive_crosses) if flag), default=-1)
    last_neg_idx = max((i for i, flag in enumerate(negative_crosses) if flag), default=-1)
    current_idx = len(recs) - 1
    positive_age = float(current_idx - last_pos_idx) if last_pos_idx >= 0 and last_pos_idx > last_neg_idx and gaps[-1] >= 0 else float("nan")
    negative_age = float(current_idx - last_neg_idx) if last_neg_idx >= 0 and last_neg_idx > last_pos_idx and gaps[-1] < 0 else float("nan")

    c = closes[-1]
    o = opens[-1]
    h = highs[-1]
    l = lows[-1]
    bar_range = max(0.0, h - l)
    close_position = (c - l) / bar_range if bar_range > 0 else 0.5
    upper_wick = max(0.0, h - max(o, c))
    upper_wick_pct = upper_wick / c if c > 0 else float("nan")
    atr14 = _atr(highs, lows, closes, 14)
    ret8 = c / closes[-9] - 1.0 if len(closes) >= 9 and closes[-9] > 0 else float("nan")
    prior40 = highs[max(0, len(highs) - 41) : -1]
    prior_high40 = max(prior40) if prior40 else float("nan")
    air_room40 = (prior_high40 - c) / c if math.isfinite(prior_high40) and c > 0 else float("nan")
    previous_vols = volumes[max(0, len(volumes) - 21) : -1]
    vol_mean20 = _mean(previous_vols)
    volume_ratio20 = volumes[-1] / vol_mean20 if math.isfinite(vol_mean20) and vol_mean20 > 0 else float("nan")
    bar_extension_atr = abs(c - o) / atr14 if math.isfinite(atr14) and atr14 > 0 else float("nan")

    # Preserve enough anchored data for later PAVWAP/retention calculation without
    # passing the full 72-row tail through the engine.
    anchor_idx = last_pos_idx if last_pos_idx >= 0 else current_idx
    anchor_slice = recs[anchor_idx:]
    cum_volume = sum(float(r["volume"]) for r in anchor_slice if float(r["volume"]) > 0)
    if cum_volume > 0:
        cum_pv = sum(((r["high"] + r["low"] + r["close"]) / 3.0) * r["volume"] for r in anchor_slice if r["volume"] > 0)
        pavwap = cum_pv / cum_volume
    else:
        pavwap = _mean([(r["high"] + r["low"] + r["close"]) / 3.0 for r in anchor_slice])
    anchor_low = min([recs[max(0, anchor_idx - 1)]["low"]] + [r["low"] for r in anchor_slice])
    highest_since = max(r["high"] for r in anchor_slice)
    move = max(0.0, highest_since - anchor_low)
    retention50 = anchor_low + 0.50 * move
    defense38 = anchor_low + 0.382 * move

    observed_dt = _parse_dt(observed_at) or _dt.datetime.now()
    last_pos = recs[last_pos_idx] if last_pos_idx >= 0 else None
    return {
        "market_key": str(market_key or ""),
        "symbol": str(symbol or "").strip().upper(),
        "name": str(name or symbol or ""),
        "observed_at": _iso(observed_dt),
        "bar_datetime": _iso(recs[-1]["date"]),
        "bar_date": recs[-1]["date"].date().isoformat(),
        "session_bar_times": [_iso(r["date"]) for r in recs if r["date"].date() == recs[-1]["date"].date()],
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": volumes[-1],
        "rsiscaled": rsiscaled[-1],
        "var3": var3[-1],
        "pulse_gap": gaps[-1],
        "pulse_gap_pct": gap_pcts[-1],
        "previous_gap": gaps[-2],
        "previous_gap_pct": gap_pcts[-2],
        "positive_cross_now": bool(positive_crosses[-1]),
        "negative_cross_now": bool(negative_crosses[-1]),
        "positive_pulse_age_bars": positive_age,
        "negative_pulse_age_bars": negative_age,
        "last_positive_cross_bar": _iso(last_pos["date"]) if last_pos else "",
        "last_positive_cross_price": float(last_pos["close"]) if last_pos else None,
        "pavwap": pavwap,
        "anchor_low": anchor_low,
        "highest_since_pulse": highest_since,
        "retention_50": retention50,
        "defense_38": defense38,
        "atr14": atr14,
        "ret8": ret8,
        "prior_high40": prior_high40,
        "air_room40": air_room40,
        "close_position": close_position,
        "upper_wick_pct": upper_wick_pct,
        "volume_ratio20": volume_ratio20,
        "bar_extension_atr": bar_extension_atr,
        "tail_rows": len(recs),
    }


@dataclass(frozen=True)
class PulseAcceptanceConfig:
    confirm_min_seconds: float = 3.0
    confirm_min_observations: int = 2
    accepted_min_seconds: float = 6.0
    accepted_min_observations: int = 3
    arm_gap_pct: float = 0.0002
    fail_gap_pct: float = -0.0002
    failure_observations: int = 2
    min_close_position: float = 0.55
    max_upper_wick_pct: float = 0.018
    max_ret8: float = 0.040
    desired_air_room40: float = 0.010
    no_chase_atr: float = 0.50
    no_chase_bar_extension_atr: float = 1.25
    first_cross_price_tolerance: float = 0.0010
    pavwap_tolerance: float = 0.0005
    active_episode_max_bars: int = 8
    technical_checks_required: int = 4


@dataclass
class PulseEpisode:
    episode_id: str
    market_key: str
    decision_lane: str
    symbol: str
    name: str
    session_date: str
    anchor_bar: str
    first_cross_at: str
    first_cross_price: float
    state: str = LIVE_CROSS
    first_published_at: str = ""
    first_published_price: Optional[float] = None
    fast_confirm_at: str = ""
    fast_confirm_price: Optional[float] = None
    accepted_at: str = ""
    trend_confirmed_at: str = ""
    last_seen_at: str = ""
    last_price: float = float("nan")
    last_gap_pct: float = float("nan")
    previous_observation_gap_pct: float = float("nan")
    last_observation_fingerprint: str = ""
    observation_count: int = 0
    stable_observation_count: int = 0
    failure_observation_count: int = 0
    below_acceptance_count: int = 0
    state_change_count: int = 0
    anchor_low: float = float("nan")
    highest_since_pulse: float = float("nan")
    pulse_avwap: float = float("nan")
    retention_50: float = float("nan")
    defense_38: float = float("nan")
    atr14: float = float("nan")
    pullback_started: bool = False
    pullback_low: float = float("nan")
    pre_pullback_high: float = float("nan")
    final_reason: str = ""
    sealed_outcome: str = ""
    sealed_at: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)


class PulseAcceptanceEngine:
    """Thread-safe intrabar episode state machine, isolated by market/lane/symbol."""

    def __init__(self, config: Optional[PulseAcceptanceConfig] = None):
        self.config = config or PulseAcceptanceConfig()
        self._lock = threading.RLock()
        self._episodes: Dict[Tuple[str, str, str], PulseEpisode] = {}
        self._session_by_context: Dict[Tuple[str, str], str] = {}
        self._sequence_by_context: Dict[Tuple[str, str, str], int] = {}

    @staticmethod
    def _key(market_key: str, decision_lane: str, symbol: str) -> Tuple[str, str, str]:
        return (str(market_key or ""), str(decision_lane or "radar").lower(), str(symbol or "").strip().upper())

    def _reset_context_if_needed(self, market_key: str, decision_lane: str, session_date: str) -> None:
        context = (str(market_key or ""), str(decision_lane or "radar").lower())
        old = self._session_by_context.get(context)
        if old and old != session_date:
            try:
                if str(session_date or "") < str(old or ""):
                    return
            except Exception:
                return
            # Remove only this market/lane.  Other markets and the model lane stay intact.
            for key in [k for k in self._episodes if k[:2] == context]:
                self._episodes.pop(key, None)
        self._session_by_context[context] = session_date

    def _next_episode_id(self, market_key: str, lane: str, symbol: str, bar_key: str) -> str:
        seq_key = self._key(market_key, lane, symbol)
        seq = int(self._sequence_by_context.get(seq_key, 0)) + 1
        self._sequence_by_context[seq_key] = seq
        safe_bar = str(bar_key or "").replace(" ", "T").replace(":", "").replace("-", "")
        return f"PAAC-{market_key}-{lane}-{symbol}-{safe_bar}-{seq}"

    def observe(
        self,
        observation: Mapping[str, Any],
        *,
        signal_bar_is_forming: bool,
        session_date: str,
        decision_lane: str = "radar",
        observed_at: Any = None,
    ) -> Optional[Dict[str, Any]]:
        obs = dict(observation or {})
        market_key = str(obs.get("market_key") or "")
        symbol = str(obs.get("symbol") or "").strip().upper()
        lane = str(decision_lane or "radar").lower()
        if not market_key or not symbol or lane != "radar" or not bool(signal_bar_is_forming):
            return None
        session_date = str(session_date or obs.get("bar_date") or "")
        if not session_date:
            return None
        now_dt = _parse_dt(observed_at) or _parse_dt(obs.get("observed_at")) or _dt.datetime.now()
        now_s = _iso(now_dt)
        bar_key = str(obs.get("bar_datetime") or "")
        price = _finite(obs.get("close"))
        gap_pct = _finite(obs.get("pulse_gap_pct"))
        if not bar_key or not math.isfinite(price) or price <= 0 or not math.isfinite(gap_pct):
            return None

        # A file watcher can report the same write more than once.  Confirmation
        # must count distinct source versions, not duplicate notifications.  Prefer
        # nanosecond mtime when available; without it, fall back to the observed bar
        # values so repeated identical payloads remain one technical observation.
        source_version = str(obs.get("source_mtime_ns") or obs.get("source_mtime") or "")
        observation_fingerprint = "|".join([
            bar_key, source_version, str(obs.get("source_size") or ""),
            f"{price:.10f}", f"{_finite(obs.get('high')):.10f}",
            f"{_finite(obs.get('low')):.10f}", f"{_finite(obs.get('volume'), 0.0):.4f}",
            f"{gap_pct:.12f}",
        ])

        with self._lock:
            current_session = self._session_by_context.get((market_key, lane))
            if current_session and str(session_date or "") < str(current_session or ""):
                return None
            self._reset_context_if_needed(market_key, lane, session_date)
            key = self._key(market_key, lane, symbol)
            episode = self._episodes.get(key)

            new_cross = bool(obs.get("positive_cross_now"))
            if episode is None or episode.state in _TERMINAL_STATES:
                # A failed/sealed episode cannot instantly respawn on the same
                # forming candle merely because the source rewrites it above zero.
                # A genuine recross is allowed only on a later bar.
                if episode is not None and episode.anchor_bar == bar_key:
                    return None
                if not new_cross:
                    return None
                episode = PulseEpisode(
                    episode_id=self._next_episode_id(market_key, lane, symbol, bar_key),
                    market_key=market_key,
                    decision_lane=lane,
                    symbol=symbol,
                    name=str(obs.get("name") or symbol),
                    session_date=session_date,
                    anchor_bar=bar_key,
                    first_cross_at=now_s,
                    first_cross_price=price,
                    first_published_at=now_s,
                    first_published_price=price,
                    last_seen_at=now_s,
                    last_price=price,
                    last_gap_pct=gap_pct,
                    observation_count=0,
                    state_change_count=1,
                    anchor_low=_finite(obs.get("anchor_low"), _finite(obs.get("low"), price)),
                    highest_since_pulse=_finite(obs.get("highest_since_pulse"), _finite(obs.get("high"), price)),
                    pulse_avwap=_finite(obs.get("pavwap"), price),
                    retention_50=_finite(obs.get("retention_50"), price),
                    defense_38=_finite(obs.get("defense_38"), price),
                    atr14=_finite(obs.get("atr14")),
                    final_reason="تم رصد تقاطع RSIScaled فوق VAR3 أثناء تعبئة الشمعة",
                )
                self._episodes[key] = episode
            elif episode.session_date != session_date:
                return None
            elif episode.anchor_bar != bar_key and episode.state == LIVE_FAILED:
                return None

            if episode.last_observation_fingerprint == observation_fingerprint:
                # Return the current truth without advancing hysteresis counters.
                # The publication layer suppresses the unchanged UI patch.
                return self._row_from_episode(episode)
            episode.last_observation_fingerprint = observation_fingerprint

            previous_state = episode.state
            previous_gap = episode.last_gap_pct
            episode.observation_count += 1
            episode.previous_observation_gap_pct = previous_gap
            episode.last_seen_at = now_s
            episode.last_price = price
            episode.last_gap_pct = gap_pct
            # L0 belongs to the pulse candle and the candle immediately before it.
            # It may evolve while the anchor candle itself is still forming, but it
            # must freeze afterwards; otherwise every later lower low would move the
            # defence down and make an L0 failure mathematically impossible.
            if episode.anchor_bar == bar_key:
                episode.anchor_low = min(
                    x for x in [episode.anchor_low, _finite(obs.get("anchor_low")), _finite(obs.get("low"))]
                    if math.isfinite(x)
                )
            episode.highest_since_pulse = max(
                x for x in [episode.highest_since_pulse, _finite(obs.get("highest_since_pulse")), _finite(obs.get("high"))]
                if math.isfinite(x)
            )
            episode.pulse_avwap = _finite(obs.get("pavwap"), episode.pulse_avwap)
            episode.atr14 = _finite(obs.get("atr14"), episode.atr14)
            move = max(0.0, episode.highest_since_pulse - episode.anchor_low)
            episode.retention_50 = episode.anchor_low + 0.50 * move
            episode.defense_38 = episode.anchor_low + 0.382 * move

            first_dt = _parse_dt(episode.first_cross_at) or now_dt
            elapsed_sec = max(0.0, (now_dt - first_dt).total_seconds())
            distance_atr = (price - episode.first_cross_price) / episode.atr14 if math.isfinite(episode.atr14) and episode.atr14 > 0 else float("nan")
            move_since_cross_pct = (price / episode.first_cross_price - 1.0) * 100.0 if episode.first_cross_price > 0 else float("nan")
            gap_slope = gap_pct - previous_gap if math.isfinite(previous_gap) else 0.0

            c = self.config
            quality_checks = {
                "gap_above_arm": gap_pct >= c.arm_gap_pct,
                "gap_not_weakening": gap_slope >= -0.00002,
                "holds_cross_price": price >= episode.first_cross_price * (1.0 - c.first_cross_price_tolerance),
                "holds_pavwap": price >= episode.pulse_avwap * (1.0 - c.pavwap_tolerance) if math.isfinite(episode.pulse_avwap) else True,
                "healthy_close": _finite(obs.get("close_position"), 0.5) >= c.min_close_position,
                "wick_not_dominant": _finite(obs.get("upper_wick_pct"), 0.0) <= c.max_upper_wick_pct,
            }
            quality_count = sum(1 for passed in quality_checks.values() if passed)
            quality_required = min(len(quality_checks), max(1, int(c.technical_checks_required)))
            stable_now = bool(
                gap_pct >= c.arm_gap_pct
                and quality_checks["holds_cross_price"]
                and quality_checks["holds_pavwap"]
                and quality_count >= quality_required
            )
            if stable_now:
                episode.stable_observation_count += 1
            else:
                # Fast confirmation means consecutive distinct stable writes, not
                # two favourable samples separated by a weak rewrite.
                episode.stable_observation_count = 0

            if gap_pct <= c.fail_gap_pct:
                episode.failure_observation_count += 1
            elif gap_pct >= 0:
                episode.failure_observation_count = 0

            below_acceptance = bool(
                (math.isfinite(episode.pulse_avwap) and price < episode.pulse_avwap * (1.0 - c.pavwap_tolerance))
                and (math.isfinite(episode.defense_38) and price < episode.defense_38)
            )
            episode.below_acceptance_count = episode.below_acceptance_count + 1 if below_acceptance else 0

            # Detect a technically healthy first pullback and its resumption.
            observed_low = _finite(obs.get("low"), price)
            pullback_depth = episode.highest_since_pulse - observed_low
            if (
                not episode.pullback_started
                and math.isfinite(episode.atr14)
                and episode.atr14 > 0
                and pullback_depth >= 0.15 * episode.atr14
                and observed_low >= episode.defense_38
            ):
                episode.pullback_started = True
                episode.pre_pullback_high = episode.highest_since_pulse
                episode.pullback_low = observed_low
            elif episode.pullback_started:
                episode.pullback_low = min(episode.pullback_low, observed_low) if math.isfinite(episode.pullback_low) else observed_low

            no_chase = bool(
                (math.isfinite(distance_atr) and distance_atr > c.no_chase_atr)
                or (_finite(obs.get("ret8")) > c.max_ret8)
                or (_finite(obs.get("bar_extension_atr")) > c.no_chase_bar_extension_atr)
            )
            hard_failure = bool(
                episode.failure_observation_count >= int(c.failure_observations)
                or price < episode.anchor_low
                or episode.below_acceptance_count >= 2
            )
            weakening = bool(
                gap_pct < 0
                or (math.isfinite(episode.pulse_avwap) and price < episode.pulse_avwap)
                or (math.isfinite(episode.retention_50) and price < episode.retention_50)
            )
            trend_resume = bool(
                episode.pullback_started
                and math.isfinite(episode.pre_pullback_high)
                and price >= episode.pre_pullback_high
                and gap_pct >= c.arm_gap_pct
                and (not math.isfinite(episode.pullback_low) or episode.pullback_low >= episode.defense_38)
            )
            episode_bar_age = _bar_age_bars(episode.anchor_bar, bar_key)
            expired = bool(int(c.active_episode_max_bars or 0) > 0 and episode_bar_age > int(c.active_episode_max_bars))

            if expired:
                episode.state = LIVE_EXPIRED
                episode.final_reason = f"انتهت مراقبة النبضة بعد {episode_bar_age} شموع؛ لا تبقى منشورة بلا اختراق/تأكيد"
            elif hard_failure:
                episode.state = LIVE_FAILED
                episode.final_reason = "فشل لحظي مؤكد: كسر فجوة الهسترة أو دفاع PAVWAP/38.2% في قراءتين"
            elif no_chase and episode.state not in {LIVE_TREND_CONFIRMED}:
                episode.state = LIVE_LATE_NO_CHASE
                episode.final_reason = "النبضة قائمة لكن السعر ممتد عن سعر التقاطع؛ لا تطارد"
            elif trend_resume:
                episode.state = LIVE_TREND_CONFIRMED
                if not episode.trend_confirmed_at:
                    episode.trend_confirmed_at = now_s
                episode.final_reason = "اندفاع ثم تراجع صحي فوق الدفاع ثم استئناف للقمة"
            elif (
                elapsed_sec >= c.accepted_min_seconds
                and episode.observation_count >= int(c.accepted_min_observations)
                and episode.stable_observation_count >= int(c.accepted_min_observations)
                and price >= max(episode.pulse_avwap, episode.retention_50)
            ):
                episode.state = LIVE_ACCEPTED
                if not episode.accepted_at:
                    episode.accepted_at = now_s
                if not episode.fast_confirm_at:
                    episode.fast_confirm_at = now_s
                    episode.fast_confirm_price = price
                episode.final_reason = "قبول لحظي: صمود متكرر فوق PAVWAP واحتفاظ 50%"
            elif (
                elapsed_sec >= c.confirm_min_seconds
                and episode.observation_count >= int(c.confirm_min_observations)
                and episode.stable_observation_count >= int(c.confirm_min_observations)
                and stable_now
            ):
                episode.state = LIVE_ARMED_FAST
                if not episode.fast_confirm_at:
                    episode.fast_confirm_at = now_s
                    episode.fast_confirm_price = price
                episode.final_reason = "تأكيد لحظي مبكر بعد قراءتين مستقرتين دون انتظار إغلاق 30 دقيقة"
            elif weakening and episode.state not in {LIVE_CROSS}:
                episode.state = LIVE_WEAKENING
                episode.final_reason = "التقاطع ما زال مسجلاً لكن القبول اللحظي يضعف"
            else:
                # Do not regress an armed/accepted episode merely because one source
                # rewrite is neutral inside the hysteresis band.
                if episode.state in {LIVE_ARMED_FAST, LIVE_ACCEPTED, LIVE_TREND_CONFIRMED} and gap_pct > c.fail_gap_pct:
                    pass
                else:
                    episode.state = LIVE_CROSS
                    episode.final_reason = "تم التقاطع الآن؛ ينتظر صمودًا قصيرًا لا شمعة كاملة"

            if episode.state != previous_state:
                episode.state_change_count += 1

            technical_quality = round(100.0 * quality_count / max(1, len(quality_checks)), 1)
            episode.payload = {
                **obs,
                "elapsed_since_cross_sec": elapsed_sec,
                "distance_from_cross_atr": distance_atr,
                "move_since_cross_pct": move_since_cross_pct,
                "gap_slope_observation": gap_slope,
                "technical_quality": technical_quality,
                "technical_quality_checks": quality_checks,
                "technical_quality_kind": "deterministic_technical_not_probability",
            }
            return self._row_from_episode(episode)

    def _row_from_episode(self, episode: PulseEpisode) -> Dict[str, Any]:
        p = dict(episode.payload or {})
        stage_ar = {
            LIVE_CROSS: "تقاطع لحظي الآن",
            LIVE_ARMED_FAST: "تأكيد لحظي مبكر",
            LIVE_ACCEPTED: "نبضة مقبولة لحظيًا",
            LIVE_TREND_CONFIRMED: "ترند معتبر لحظيًا",
            LIVE_WEAKENING: "النبضة تضعف",
            LIVE_FAILED: "فزة فشلت لحظيًا",
            LIVE_LATE_NO_CHASE: "نبضة ممتدة — لا تطارد",
            LIVE_EXPIRED: "انتهت مراقبة النبضة",
            SEALED_BY_RADAR: "حُسمت بالرادار المختوم",
        }.get(episode.state, episode.state)
        publishable = is_publishable_fast_pulse_state(episode.state)
        if episode.state == LIVE_CROSS:
            professional_type = "FAST_PULSE_MOMENT"
            professional_type_ar = "لحظة النبضة"
        elif episode.state in {LIVE_ARMED_FAST, LIVE_ACCEPTED}:
            professional_type = "FAST_PULSE_QUICK_CONFIRMATION"
            professional_type_ar = "تأكيد نبضة سريع"
        else:
            professional_type = "SUPPRESSED_FAST_PULSE_STATE"
            professional_type_ar = "حالة نبضة غير منشورة"
        confirmed_delay = None
        if episode.fast_confirm_at:
            a = _parse_dt(episode.first_cross_at)
            b = _parse_dt(episode.fast_confirm_at)
            if a and b:
                confirmed_delay = max(0.0, (b - a).total_seconds())
        return {
            "id": episode.episode_id,
            "pulse_episode_id": episode.episode_id,
            "market_key": episode.market_key,
            "decision_market_key": episode.market_key,
            "decision_lane": episode.decision_lane,
            "symbol": episode.symbol,
            "name": episode.name,
            "timeframe": "30m",
            "source": "مؤشر رسوخ النبضة — مسار Price Tape الخفيف",
            "radar_scope": "LIVE_FAST_POST_SEAL" if episode.sealed_outcome else "LIVE_FAST_PREVIEW",
            "radar_stage": episode.state,
            "pulse_acceptance_state": episode.state,
            "pulse_acceptance_state_ar": stage_ar,
            "pulse_state_ar": stage_ar,
            "professional_signal_family": "FAST_PULSE" if publishable else "SUPPRESSED",
            "professional_signal_type": professional_type,
            "professional_signal_type_ar": professional_type_ar,
            "two_signal_gate_passed": bool(publishable),
            "status": "رسوخ لحظي مستمر بعد ختم شمعة الرادار" if episode.sealed_outcome else "مراقبة لحظية غير مختومة",
            "stock_rating": stage_ar,
            "action_state": episode.state,
            "recommendation_datetime": episode.anchor_bar,
            "signal_bar_time": episode.anchor_bar,
            "signal_bar_is_sealed": bool(episode.sealed_outcome),
            "signal_detected_at": episode.first_cross_at,
            "first_cross_at": episode.first_cross_at,
            "first_cross_price": episode.first_cross_price,
            "fast_confirm_at": episode.fast_confirm_at or None,
            "fast_confirm_price": episode.fast_confirm_price,
            "fast_confirm_delay_sec": confirmed_delay,
            "appearance_at": episode.first_published_at,
            "appearance_price": episode.first_published_price,
            "current_price": episode.last_price,
            "last_update": episode.last_seen_at,
            "entry_status": "NOT_OFFICIAL_INTRABAR",
            "entry_price": None,
            "live_publishable": False,
            "_ain_official": False,
            "radar_official_evaluation": False,
            "official_rule_passed": False,
            "not_for_official_statistics": True,
            "tradable": False,
            "truth_scope": "INTRABAR_FIRST_OBSERVED",
            "cross_time_precision": "first_process_observation_not_exchange_tick",
            "observation_count": episode.observation_count,
            "episode_bar_age": _bar_age_bars(episode.anchor_bar, episode.payload.get("bar_datetime") or episode.anchor_bar),
            "active_episode_max_bars": self.config.active_episode_max_bars,
            "distinct_observation_count": episode.observation_count,
            "source_observation_fingerprint": episode.last_observation_fingerprint,
            "stable_observation_count": episode.stable_observation_count,
            "failure_observation_count": episode.failure_observation_count,
            "state_change_count": episode.state_change_count,
            "pulse_avwap": episode.pulse_avwap,
            "retention_50": episode.retention_50,
            "defense_38": episode.defense_38,
            "anchor_low": episode.anchor_low,
            "highest_since_pulse": episode.highest_since_pulse,
            "atr14": episode.atr14,
            "move_since_cross_pct": p.get("move_since_cross_pct"),
            "distance_from_cross_atr": p.get("distance_from_cross_atr"),
            "pulse_gap_pct": p.get("pulse_gap_pct"),
            "pulse_gap_slope_observation": p.get("gap_slope_observation"),
            "rsiscaled": p.get("rsiscaled"),
            "var3": p.get("var3"),
            "close_position": p.get("close_position"),
            "upper_wick_pct": p.get("upper_wick_pct"),
            "ret8": p.get("ret8"),
            "air_room40": p.get("air_room40"),
            "volume_ratio20": p.get("volume_ratio20"),
            "technical_quality": p.get("technical_quality"),
            "confidence": p.get("technical_quality"),
            "confidence_kind": "technical_not_probability",
            "var3_gann_category_text": episode.final_reason,
            "pulse_acceptance_reason": episode.final_reason,
            "accepted_at": episode.accepted_at or None,
            "trend_confirmed_at": episode.trend_confirmed_at or None,
            "pullback_started": episode.pullback_started,
            "pullback_low": episode.pullback_low if math.isfinite(episode.pullback_low) else None,
            "sealed_outcome": episode.sealed_outcome or None,
            "sealed_at": episode.sealed_at or None,
            "fast_monitor_continues_after_seal": bool(episode.sealed_outcome),
        }

    def active_rows(self, market_key: str, *, decision_lane: str = "radar", session_date: Optional[str] = None) -> List[Dict[str, Any]]:
        lane = str(decision_lane or "radar").lower()
        with self._lock:
            rows: List[Dict[str, Any]] = []
            for (mkt, ln, _), episode in self._episodes.items():
                if mkt != str(market_key or "") or ln != lane:
                    continue
                if session_date and episode.session_date != str(session_date):
                    continue
                if episode.state == SEALED_BY_RADAR:
                    continue
                if not is_publishable_fast_pulse_state(episode.state):
                    continue
                rows.append(self._row_from_episode(episode))
            priority = {
                LIVE_TREND_CONFIRMED: 0,
                LIVE_ACCEPTED: 1,
                LIVE_ARMED_FAST: 2,
                LIVE_CROSS: 3,
                LIVE_WEAKENING: 4,
                LIVE_LATE_NO_CHASE: 5,
                LIVE_FAILED: 6,
            }
            rows.sort(key=lambda r: (priority.get(str(r.get("radar_stage")), 9), str(r.get("signal_detected_at") or "")), reverse=False)
            return rows

    def truth_for_symbol(self, market_key: str, symbol: str, *, decision_lane: str = "radar") -> Optional[Dict[str, Any]]:
        with self._lock:
            episode = self._episodes.get(self._key(market_key, decision_lane, symbol))
            return self._row_from_episode(episode) if episode is not None else None

    def mark_sealed(
        self,
        market_key: str,
        symbol: str,
        *,
        bar_time: Any,
        sealed_stage: str,
        decision_lane: str = "radar",
        sealed_at: Any = None,
    ) -> bool:
        with self._lock:
            episode = self._episodes.get(self._key(market_key, decision_lane, symbol))
            if episode is None:
                return False
            bar_a = str(episode.anchor_bar or "")[:16]
            bar_b = _iso(bar_time)[:16]
            if not bar_a or not bar_b or bar_a != bar_b:
                return False
            episode.sealed_outcome = str(sealed_stage or "SEALED")
            episode.sealed_at = _iso(sealed_at or _dt.datetime.now())
            # Sealing is a separate official fact, not the end of the technical
            # continuation episode.  Keep monitoring PAVWAP/pullback/trend until
            # failure or the market-specific session reset.
            episode.payload["sealed_outcome"] = episode.sealed_outcome
            episode.payload["sealed_at"] = episode.sealed_at
            return True

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self.config),
                "sessions": {f"{k[0]}|{k[1]}": v for k, v in self._session_by_context.items()},
                "episodes": {"|".join(k): asdict(v) for k, v in self._episodes.items()},
            }
