# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import threading
import time
from pathlib import Path

import pandas as pd

from probability_resolution_authority import (
    CONTRACT,
    US_RESOLUTION_CONTRACT,
    classify_with_parent_resolution_authority,
)

ROOT = Path(__file__).resolve().parents[1]
US = "local_السوق الأمريكي"
BAR = "2026-09-04 11:00"


def _authority(**updates):
    value = {
        "contract": CONTRACT,
        "built_by": "PARENT",
        "market_key": US,
        "bar_key": BAR,
        "session_pinned": True,
        "session_key": "US-2026-09-04",
        "universe_hash": "a" * 64,
        "trusted_universe_count": 20,
        "exact_target_count": 19,
        "exact_coverage_ratio": 0.95,
        "source_scan_scanned": 20,
        "source_scan_total": 20,
        "source_scan_complete": True,
        "confirmed_no_new_bar_count": 1,
        "resolved_count": 20,
        "pending_count": 0,
        "resolution_complete": True,
        "resolution_status": "RESOLVED",
        "resolution_contract": US_RESOLUTION_CONTRACT,
    }
    value.update(updates)
    return value


def _symbol_rows(symbol: str, *, base: float, cross: bool, periods: int = 64):
    start = pd.Timestamp("2026-07-29 10:00:00")
    if cross:
        closes = [base - 0.1 * index for index in range(periods - 1)]
        closes.append(closes[-1] + 0.8)
    else:
        closes = [base * (1.0 + 0.00035 * i + 0.0015 * math.sin(i / 4.0)) for i in range(periods)]
    rows = []
    for index, close in enumerate(closes):
        open_price = close * (0.9995 if index % 2 == 0 else 1.0003)
        rows.append({
            "date": start + pd.Timedelta(minutes=30 * index),
            "symbol": symbol,
            "open": open_price,
            "high": max(open_price, close) * 1.002,
            "low": min(open_price, close) * 0.998,
            "close": close,
            "volume": 10_000.0 + index * 31.0,
            "name": symbol,
        })
    return rows


def _market(universe: int = 14):
    target = "F32T"
    target_rows = _symbol_rows(target, base=100.0, cross=True)
    all_rows = list(target_rows)
    for index in range(1, universe):
        all_rows.extend(_symbol_rows(f"F{index:02d}", base=45.0 + index * 1.5, cross=False))
    frame = pd.DataFrame(all_rows).sort_values(["symbol", "date"], kind="stable").reset_index(drop=True)
    return frame, target, {target: target_rows}


def _authority_for_market_records(records, exact: int, total: int):
    bar = pd.Timestamp(next(iter(records.values()))[-1]["date"]).strftime("%Y-%m-%d %H:%M")
    return _authority(
        bar_key=bar,
        trusted_universe_count=total,
        exact_target_count=exact,
        exact_coverage_ratio=float(exact) / float(total),
        source_scan_scanned=total,
        source_scan_total=total,
        confirmed_no_new_bar_count=total - exact,
        resolved_count=total,
    )


def test_f32_valid_19_of_20_plus_one_resolved_promotes_market_wide():
    out = classify_with_parent_resolution_authority(
        market_key=US, bar_time=BAR, model_target_count=19, model_total_symbols=19,
        authority=_authority(),
    )
    assert out["probability_kind"] == "MARKET_WIDE_CONFIRMED"
    assert out["coverage_target_count"] == 19
    assert out["coverage_total_symbols"] == 20
    assert out["authority_valid"] is True


def test_f32_us_worker_cannot_self_promote_without_parent_authority():
    out = classify_with_parent_resolution_authority(
        market_key=US, bar_time=BAR, model_target_count=19, model_total_symbols=19,
        authority=None,
    )
    assert out["probability_kind"] == "PARTIAL_MARKET_PROVISIONAL"
    assert out["authority_reason"] == "AUTHORITY_MISSING"


def test_f32_market_mismatch_is_fail_closed():
    out = classify_with_parent_resolution_authority(
        market_key=US, bar_time=BAR, model_target_count=19, model_total_symbols=19,
        authority=_authority(market_key="local_السوق السعودي"),
    )
    assert out["probability_kind"] == "PARTIAL_MARKET_PROVISIONAL"
    assert out["authority_reason"] == "AUTHORITY_MARKET_MISMATCH"


def test_f32_bar_mismatch_is_fail_closed():
    out = classify_with_parent_resolution_authority(
        market_key=US, bar_time=BAR, model_target_count=19, model_total_symbols=19,
        authority=_authority(bar_key="2026-09-04 10:30"),
    )
    assert out["probability_kind"] == "PARTIAL_MARKET_PROVISIONAL"
    assert out["authority_reason"] == "AUTHORITY_BAR_MISMATCH"


def test_f32_missing_session_pin_or_hash_cannot_promote():
    for broken in (_authority(session_pinned=False), _authority(universe_hash=""), _authority(session_key="")):
        out = classify_with_parent_resolution_authority(
            market_key=US, bar_time=BAR, model_target_count=19, model_total_symbols=19,
            authority=broken,
        )
        assert out["probability_kind"] == "PARTIAL_MARKET_PROVISIONAL"
        assert out["authority_valid"] is False


def test_f32_f29_refresh_gate_stops_retry_loop_only_with_valid_resolution():
    from live_monitor_engine_pkg._header import _h1r12_snapshot_refresh_requirement
    from probability_hot_path import h1r12_market_snapshot_authority_from_frame

    rows = []
    target = pd.Timestamp(BAR)
    for i in range(19):
        rows.append({"date": target, "symbol": f"U{i:03d}"})
    rows.extend([
        {"date": target, "symbol": "SPX"},
        {"date": target, "symbol": "DJI"},
    ])
    snapshot = pd.DataFrame(rows)
    proof = h1r12_market_snapshot_authority_from_frame(
        snapshot, target, US, resolution_authority=_authority()
    )
    decision = _h1r12_snapshot_refresh_requirement(US, proof)
    assert proof["probability_kind"] == "MARKET_WIDE_CONFIRMED"
    assert proof["market_target_bar_coverage_ratio"] == 0.95
    assert proof["market_context_indices_fresh"] is True
    assert decision["required"] is False


def test_f32_fallback_scorer_keeps_real_model_rows_but_uses_resolved_denominator():
    from live_pulse_seal_engine import estimate_market_live_probability_batch

    frame, target, records = _market(14)
    auth = _authority_for_market_records(records, exact=14, total=15)
    result = estimate_market_live_probability_batch(
        frame, records, market_key=US, resolution_authority=auth
    )[target]
    assert result["available"] is True
    assert result["probability_kind"] == "MARKET_WIDE_CONFIRMED"
    assert result["market_model_snapshot_symbols"] == 14
    assert result["market_snapshot_symbols"] == 15
    assert result["market_target_bar_symbols"] == 14
    assert result["probability_resolution_authority_valid"] is True


def test_f32_parent_builder_binds_market_bar_pin_hash_and_resolution():
    from live_monitor_engine_pkg._mixin_ranker_ml import _RankerMLMixin

    class Parent:
        def _local_market_name(self, _key): return "السوق الأمريكي"
        def _bar_freshness_from_df(self, _df, bar, _key):
            return {
                "trusted_universe_pinned": True,
                "target_bar_exact_count": 19,
                "target_bar_exact_ratio": 0.95,
                "fresh_symbols": {f"U{i:03d}" for i in range(19)},
                "source_scan_scanned": 20,
                "source_scan_total": 20,
                "source_scan_complete": True,
                "confirmed_no_new_bar_count": 1,
                "snapshot_resolved_count": 20,
                "snapshot_resolution_pending_count": 0,
                "snapshot_resolution_complete": True,
                "snapshot_resolution_status": "RESOLVED",
                "snapshot_resolution_contract": US_RESOLUTION_CONTRACT,
            }
        def _session_pinned_universe_entry(self, _key):
            return {"session_key": "S", "hash": "b" * 64, "count": 20}

    records = {"U000": [{"date": pd.Timestamp(BAR), "symbol": "U000"}]}
    auth = _RankerMLMixin._h1r13_r4_parent_probability_resolution_authority(
        Parent(), pd.DataFrame(records["U000"]), records, market_key=US
    )
    assert auth["contract"] == CONTRACT
    assert auth["built_by"] == "PARENT"
    assert auth["bar_key"] == BAR
    assert auth["trusted_universe_count"] == 20
    assert auth["resolved_count"] == 20
    assert auth["resolution_contract"] == US_RESOLUTION_CONTRACT


def test_f32_isolated_worker_ipc_carries_parent_resolution_authority(tmp_path):
    from isolated_probability_worker import IsolatedProbabilityWorker

    frame, target, records = _market(14)
    auth = _authority_for_market_records(records, exact=14, total=15)
    worker = IsolatedProbabilityWorker(
        ROOT, work_dir=tmp_path / "ipc", log_dir=tmp_path / "logs"
    )
    try:
        result, _meta = worker.score_batch(
            frame, records, market_key=US, timeout_sec=30.0,
            resolution_authority=auth,
        )
    finally:
        worker.close()
    row = result[target]
    assert row["available"] is True
    assert row["probability_kind"] == "MARKET_WIDE_CONFIRMED"
    assert row["market_model_snapshot_symbols"] == 14
    assert row["market_snapshot_symbols"] == 15
    assert row["probability_resolution_authority_contract"] == CONTRACT
    assert row["probability_resolution_authority_valid"] is True


def _resolver_owner(tmp_path, *, pending=False, source_stale=False):
    from live_monitor_engine_pkg._mixin_compute import _ComputeMixin

    missing = "U019"
    source = tmp_path / "U019.dat"
    source.write_bytes(b"stable-f32-source")

    class Owner(_ComputeMixin):
        def __init__(self):
            self._active_market_key = US
            self._snapshot_resolution_lock = threading.RLock()
            self._snapshot_resolution_by_state = {}
            self._last_snapshot_resolution_by_market = {}
            self._r12_source_watchdog_status_by_market = {
                US: {"source_stale": bool(source_stale), "allow_official_birth": not bool(source_stale)}
            }
            self._live_sniper_source_pending_by_market = {US: ({"pending": {}} if pending else {})}
            self._live_sniper_source_running_by_market = set()
            self._live_sniper_source_inflight_ids_by_market = {}
        def _local_market_name(self, _key): return "السوق الأمريكي"
        def _session_pinned_universe_entry(self, _key):
            syms = [f"U{i:03d}" for i in range(20)]
            return {
                "symbols": syms, "count": 20, "hash": "c" * 64, "session_key": "S",
                "rows_by_symbol": {missing: {"filename": str(source), "fields": 8}},
            }
        def _read_metastock_last_records_light(self, *_args, **_kwargs):
            return [
                {"date": "2026-09-04 10:00"},
                {"date": "2026-09-04 10:30"},
            ]

    return Owner(), {f"U{i:03d}" for i in range(19)}, missing


def test_f32_us_resolver_never_declares_absence_while_source_work_is_pending(tmp_path):
    owner, exact, missing = _resolver_owner(tmp_path, pending=True)
    out = owner._resolve_us_snapshot_missing_from_source(US, BAR, exact)
    assert out["snapshot_resolution_status"] == "SOURCE_PENDING_EXPORT"
    assert out["snapshot_resolution_complete"] is False
    assert out["confirmed_no_new_bar_count"] == 0
    assert out["snapshot_resolution_forensic_by_symbol"][missing] == "SOURCE_PENDING_EXPORT"


def test_f32_us_resolver_blocks_unhealthy_source_watchdog(tmp_path):
    owner, exact, missing = _resolver_owner(tmp_path, source_stale=True)
    out = owner._resolve_us_snapshot_missing_from_source(US, BAR, exact)
    assert out["snapshot_resolution_status"] == "SOURCE_OFFLINE_WAITING"
    assert out["snapshot_resolution_complete"] is False
    assert out["snapshot_resolution_forensic_by_symbol"][missing] == "SOURCE_WATCHDOG_NOT_OFFICIAL"


def test_f32_us_resolver_requires_bounded_stable_identity_then_proves_absence(tmp_path, monkeypatch):
    owner, exact, missing = _resolver_owner(tmp_path)
    monkeypatch.setenv("AIN_US_NO_TARGET_SETTLED_CONFIRM_SEC", "0.05")
    first = owner._resolve_us_snapshot_missing_from_source(US, BAR, exact)
    assert first["snapshot_resolution_complete"] is False
    assert first["snapshot_resolution_forensic_by_symbol"][missing] == "US_SETTLED_WAVE_STABILITY_DWELL_PENDING"
    time.sleep(0.06)
    second = owner._resolve_us_snapshot_missing_from_source(US, BAR, exact)
    assert second["snapshot_resolution_complete"] is True
    assert second["snapshot_resolved_count"] == 20
    assert second["confirmed_no_new_bar_count"] == 1
    assert second["snapshot_resolution_forensic_by_symbol"][missing] == "US_SETTLED_MARKET_WAVE_STABLE_ABSENCE"


def test_f32_heavy_radar_us_uses_90pct_only_after_full_resolution(tmp_path, monkeypatch):
    from live_monitor_engine_pkg._mixin_radar import _RadarMixin

    data_path = tmp_path / "market.parquet"
    data_path.write_bytes(b"x")
    captured = {}
    emitted = []

    class Signal:
        def emit(self, *args, **kwargs): emitted.append((args, kwargs))

    class Radar(_RadarMixin):
        def __init__(self, pending):
            self._stop_requested = False
            self._active_market_key = US
            self.log_message_signal = Signal()
            self.pending = pending
        def _evaluate_session_guard(self, _path, _market): return {}
        def _local_market_name(self, _market): return "السوق الأمريكي"
        def _bar_freshness_from_parquet(self, _path, _bar, _market):
            return {
                "confirmed_bar_key": BAR,
                "source_scan_scanned": 20 if not self.pending else 19,
                "source_scan_total": 20,
                "source_scan_complete": not self.pending,
                "confirmed_no_new_bar_count": 1 if not self.pending else 0,
                "snapshot_resolved_count": 20 if not self.pending else 19,
                "snapshot_resolution_pending_count": 0 if not self.pending else 1,
                "snapshot_resolution_complete": not self.pending,
                "snapshot_resolution_status": "RESOLVED" if not self.pending else "SOURCE_PENDING_EXPORT",
                "snapshot_resolution_contract": US_RESOLUTION_CONTRACT,
                "snapshot_resolution_exact_ratio": 0.95,
            }
        def _trusted_radar_universe_guard(self, guard, _market):
            out = dict(guard)
            out.update({
                "trusted_universe_count": 20,
                "trusted_universe_session_pinned": True,
                "trusted_universe_session_key": "S",
                "trusted_universe_pin_hash": "d" * 64,
            })
            return out
        def _snapshot_epoch(self): return 1
        def _radar_heavy_semantic_key(self, _path, _market, guard):
            captured.clear(); captured.update(dict(guard)); return ""

    calls = []
    def _sealed(*_a, **_k):
        calls.append((_a, _k))
        return {
            "status": "VERIFIED", "sha256": "e" * 64, "observed_rows": 19,
            "rows": 19, "expected_rows": 20, "coverage_ratio": 0.95,
            "completion_progress": False, "changed": False, "source": "test",
        }
    method_globals = _RadarMixin._schedule_live_30m_radar.__globals__
    monkeypatch.setitem(method_globals, "acceptance_runtime_market_key", lambda: "")
    monkeypatch.setitem(method_globals, "_a114_sealed_bar_content_identity", _sealed)

    good = Radar(False)
    assert good._schedule_live_30m_radar(
        str(data_path), US,
        guard={"phase": "open_live", "confirmed_bar_key": BAR},
        source_reason="F32_TEST", force=False,
    ) is False
    assert calls
    assert captured["heavy_radar_resolution_ok"] is True
    assert captured["heavy_radar_exact_model_floor"] == 0.90
    assert captured["heavy_radar_exact_model_floor_ok"] is True
    assert captured["heavy_radar_resolution_contract"] == "H1R13_R4_F32_US_RESOLVED_SNAPSHOT_V1"

    captured.clear()
    bad = Radar(True)
    assert bad._schedule_live_30m_radar(
        str(data_path), US,
        guard={"phase": "open_live", "confirmed_bar_key": BAR},
        source_reason="F32_TEST", force=False,
    ) is False
    assert captured == {}
