# -*- coding: utf-8 -*-
from __future__ import annotations

import inspect
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

US = "local_السوق الأمريكي"
SA = "local_السوق السعودي"
FX = "local_fx"


def _wait_until(predicate, timeout=4.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.004)
    return bool(predicate())


def _task(label, *, market=SA, norm=None, symbol=None, bucket=0, run=None,
          cancel=None, generation=1, neutral=False, barrier_neutral=False,
          workers=8, reserve=4):
    from live_source_processing_scheduler import SourceProcessingTask
    norm = norm or f"{label}.DAT"
    symbol = symbol if symbol is not None else label
    return SourceProcessingTask(
        market, norm, bucket, time.monotonic(),
        run or (lambda _w: None), cancel or (lambda _r: None),
        generation_seq=generation, coalescible=neutral,
        barrier_coalescible=barrier_neutral,
        generation_identity=f"h8:{generation}", symbol=symbol,
        parallelism_hint=workers, protected_reserve_hint=reserve,
    )


def _close(s, timeout=6.0):
    s.stop_after_drain()
    assert s.quiesce(timeout), s.snapshot()
    snap=s.snapshot()
    assert snap["pending"] == 0, snap
    assert snap["inflight"] == 0, snap
    assert snap["task_conservation_valid"] is True, snap
    assert snap["task_conservation_delta"] == 0, snap
    return snap


def test_h8_01_legacy_direct_saudi_capacity_contract_remains_serial_for_h7_compatibility(monkeypatch):
    import live_source_processing_scheduler as mod
    monkeypatch.delenv("AIN_LIVE_SOURCE_SAUDI_DAT_WORKERS", raising=False)
    monkeypatch.delenv("AIN_LIVE_SOURCE_SAUDI_DAT_PROTECTED_RESERVE", raising=False)
    assert mod.source_processing_capacity_hints(SA, "F1.DAT", "metastock") == (1, 0)


def test_h8_02_production_opt_in_enables_saudi_local_dat_8_workers_4_reserve(monkeypatch):
    import live_source_processing_scheduler as mod
    monkeypatch.delenv("AIN_LIVE_SOURCE_SAUDI_DAT_WORKERS", raising=False)
    monkeypatch.delenv("AIN_LIVE_SOURCE_SAUDI_DAT_PROTECTED_RESERVE", raising=False)
    assert mod.source_processing_capacity_hints(
        SA, "F1.DAT", "metastock", saudi_capacity_enabled=True
    ) == (8, 4)


def test_h8_03_saudi_capacity_overrides_are_bounded_and_reserve_cannot_consume_all_workers(monkeypatch):
    import live_source_processing_scheduler as mod
    monkeypatch.setenv("AIN_LIVE_SOURCE_SAUDI_DAT_WORKERS", "999")
    monkeypatch.setenv("AIN_LIVE_SOURCE_SAUDI_DAT_PROTECTED_RESERVE", "999")
    assert mod.source_processing_capacity_hints(SA,"F1.DAT","metastock",saudi_capacity_enabled=True) == (16,15)
    monkeypatch.setenv("AIN_LIVE_SOURCE_SAUDI_DAT_WORKERS", "1")
    monkeypatch.setenv("AIN_LIVE_SOURCE_SAUDI_DAT_PROTECTED_RESERVE", "9")
    assert mod.source_processing_capacity_hints(SA,"F1.DAT","metastock",saudi_capacity_enabled=True) == (1,0)
    monkeypatch.setenv("AIN_LIVE_SOURCE_SAUDI_DAT_WORKERS", "bad")
    monkeypatch.setenv("AIN_LIVE_SOURCE_SAUDI_DAT_PROTECTED_RESERVE", "bad")
    assert mod.source_processing_capacity_hints(SA,"F1.DAT","metastock",saudi_capacity_enabled=True) == (8,4)


def test_h8_04_runtime_explicitly_opts_into_saudi_capacity_contract():
    import live_sniper_source_runtime as runtime
    text=inspect.getsource(runtime.run_source_lane_worker)
    assert "saudi_capacity_enabled=True" in text
    assert "parallelism_hint=int(_capacity_workers)" in text
    assert "protected_reserve_hint=int(_capacity_reserve)" in text
    assert 'symbol=str(observation.get("symbol")' in text


def test_h8_05_eight_distinct_saudi_symbols_can_be_inflight_concurrently():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); release=threading.Event(); started=set(); lock=threading.Lock()
    def run(label):
        def _r(_w):
            with lock: started.add(label)
            release.wait(3)
        return _r
    for i in range(8): assert s.enqueue(_task(f"SA{i:03d}", run=run(i)))
    assert _wait_until(lambda:len(started)==8,2), (started,s.snapshot())
    snap=s.snapshot(); assert snap["inflight"] == 8, snap
    assert snap["market_inflight"].get(SA) == 8, snap
    release.set(); snap=_close(s)
    assert snap["inflight_max"] == 8, snap


def test_h8_06_saudi_idle_reserve_is_borrowable_with_one_emergency_slot():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); release=threading.Event(); started=[]; lock=threading.Lock()
    def run(label):
        def _r(_w):
            with lock: started.append(label)
            release.wait(3)
        return _r
    for i in range(12):
        assert s.enqueue(_task(f"N{i}", bucket=3, neutral=True, run=run(i)))
    assert _wait_until(lambda:len(started)==7,2), (started,s.snapshot())
    time.sleep(0.08)
    assert len(started)==7, s.snapshot()
    snap=s.snapshot(); assert snap["neutral_inflight"] == 7, snap
    assert snap["protected_reserve"] == 4, snap
    assert snap["effective_protected_reserve"] == 1, snap
    release.set(); _close(s)


def test_h8_07_protected_decision_enters_while_four_saudi_neutrals_are_blocked():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); release=threading.Event(); started=[]; lock=threading.Lock()
    def neutral(label):
        def _r(_w):
            with lock: started.append(label)
            release.wait(3)
        return _r
    for i in range(10): assert s.enqueue(_task(f"N{i}",bucket=3,neutral=True,run=neutral(i)))
    assert _wait_until(lambda:len(started)==7,2), s.snapshot()
    decision=threading.Event()
    assert s.enqueue(_task("DECISION",bucket=0,neutral=False,run=lambda _w:decision.set()))
    assert decision.wait(1), s.snapshot()
    snap=s.snapshot(); assert snap["market_reserve_seen"].get(SA)==4, snap
    release.set(); _close(s)


def test_h8_08_same_saudi_dat_path_remains_strictly_serialized():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); first=threading.Event(); release=threading.Event(); second=threading.Event()
    assert s.enqueue(_task("A",norm="F10.DAT",symbol="1010",run=lambda _w:(first.set(),release.wait(2))))
    assert first.wait(1)
    assert s.enqueue(_task("B",norm="F10.DAT",symbol="1020",generation=2,run=lambda _w:second.set()))
    time.sleep(0.08); assert not second.is_set(), s.snapshot()
    release.set(); assert second.wait(1); _close(s)


def test_h8_09_same_saudi_symbol_across_different_dat_paths_remains_serialized():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); first=threading.Event(); release=threading.Event(); second=threading.Event()
    assert s.enqueue(_task("A",norm="DIR1_F10.DAT",symbol="1120",run=lambda _w:(first.set(),release.wait(2))))
    assert first.wait(1)
    assert s.enqueue(_task("B",norm="DIR2_F77.DAT",symbol="1120",generation=2,run=lambda _w:second.set()))
    time.sleep(0.08); assert not second.is_set(), s.snapshot()
    release.set(); assert second.wait(1); _close(s)


def test_h8_10_generation_ordering_for_one_saudi_dat_is_preserved():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); first=threading.Event(); release=threading.Event(); ran=[]
    assert s.enqueue(_task("G1",norm="F20.DAT",symbol="1210",generation=101,
                           run=lambda _w:(first.set(),release.wait(2),ran.append(101))))
    assert first.wait(1)
    for gen in (102,103,104):
        assert s.enqueue(_task(f"G{gen}",norm="F20.DAT",symbol="1210",generation=gen,
                               run=lambda _w,g=gen:ran.append(g)))
    release.set(); assert _wait_until(lambda:len(ran)==4,2), (ran,s.snapshot())
    _close(s); assert ran == [101,102,103,104], ran


def test_h8_11_218_symbol_saudi_protected_burst_conserves_all_tasks_and_uses_capacity():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); lock=threading.Lock(); inflight=0; peak=0; done=0
    def run(_label):
        def _r(_w):
            nonlocal inflight,peak,done
            with lock:
                inflight += 1; peak=max(peak,inflight)
            time.sleep(0.0015)
            with lock:
                inflight -= 1; done += 1
        return _r
    for i in range(218):
        assert s.enqueue(_task(f"S{i:03d}",symbol=f"{1000+i}",norm=f"F{i:03d}.DAT",run=run(i)))
    assert _wait_until(lambda:done==218,5), (done,s.snapshot())
    snap=_close(s)
    assert peak == 8, (peak,snap)
    assert snap["completed"] == 218, snap
    assert snap["task_conservation_valid"] is True, snap


def test_h8_12_neutral_storm_plus_protected_burst_keeps_protected_lane_available():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); release=threading.Event(); neutral_started=[]; lock=threading.Lock()
    def neutral(i):
        def _r(_w):
            with lock: neutral_started.append(i)
            release.wait(4)
        return _r
    for i in range(32): assert s.enqueue(_task(f"N{i}",bucket=3,neutral=True,run=neutral(i)))
    # Demand-aware H1R5 reserve lends three idle reserve slots to neutral work,
    # while one emergency slot remains immediately claimable by protected work.
    assert _wait_until(lambda:len(neutral_started)==7,2), s.snapshot()
    protected_done=[]
    for i in range(24):
        assert s.enqueue(_task(f"P{i}",bucket=i%3,neutral=False,
                               run=lambda _w,i=i:protected_done.append(i)))
    assert _wait_until(lambda:len(protected_done)==24,3), (len(protected_done),s.snapshot())
    snap=s.snapshot(); assert snap["protected_queue_wait_ms_max"] < 3000, snap
    release.set(); _close(s)


def test_h8_13_us_pool_does_not_inflate_saudi_above_its_8_worker_fence():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); us_release=threading.Event(); us_started=[]; lock=threading.Lock()
    for i in range(16):
        def _u(_w,i=i):
            with lock: us_started.append(i)
            us_release.wait(3)
        assert s.enqueue(_task(f"U{i}",market=US,workers=16,reserve=8,run=_u))
    assert _wait_until(lambda:len(us_started)==16,2), s.snapshot()
    us_release.set(); assert _wait_until(lambda:s.snapshot()["inflight"]==0,2), s.snapshot()
    sa_release=threading.Event(); sa_started=[]
    for i in range(12):
        def _sa(_w,i=i):
            with lock: sa_started.append(i)
            sa_release.wait(3)
        assert s.enqueue(_task(f"SA{i}",workers=8,reserve=4,run=_sa))
    assert _wait_until(lambda:len(sa_started)==8,2), s.snapshot(); time.sleep(0.08)
    assert len(sa_started)==8, s.snapshot()
    sa_release.set(); snap=_close(s)
    assert snap["market_parallelism_seen"].get(SA)==8, snap


def test_h8_14_saudi_pool_does_not_leak_parallelism_into_fx():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); warm=[]
    for i in range(8): assert s.enqueue(_task(f"W{i}",run=lambda _w,warm=warm,i=i:warm.append(i)))
    assert _wait_until(lambda:len(warm)==8,2), s.snapshot()
    first=threading.Event(); release=threading.Event(); second=threading.Event()
    assert s.enqueue(_task("FX1",market=FX,workers=1,reserve=0,run=lambda _w:(first.set(),release.wait(2))))
    assert first.wait(1)
    assert s.enqueue(_task("FX2",market=FX,workers=1,reserve=0,run=lambda _w:second.set()))
    time.sleep(0.08); assert not second.is_set(), s.snapshot()
    release.set(); assert second.wait(1)
    snap=_close(s); assert snap["market_parallelism_seen"].get(FX)==1, snap


def test_h8_15_us_contract_is_unchanged_and_saudi_api_is_not_parallelized(monkeypatch):
    import live_source_processing_scheduler as mod
    monkeypatch.delenv("AIN_LIVE_SOURCE_US_DAT_WORKERS", raising=False)
    monkeypatch.delenv("AIN_LIVE_SOURCE_US_DAT_PROTECTED_RESERVE", raising=False)
    assert mod.source_processing_capacity_hints(US,"F1.DAT","metastock",saudi_capacity_enabled=True)==(16,8)
    assert mod.source_processing_capacity_hints("api_السوق السعودي","F1.DAT","api",saudi_capacity_enabled=True)==(1,0)
    assert mod.source_processing_capacity_hints(SA,"feed.json","local",saudi_capacity_enabled=True)==(1,0)


def test_h8_16_mixed_saudi_shutdown_preserves_conservation_and_joins_workers():
    from live_source_processing_scheduler import SourceProcessingScheduler
    s=SourceProcessingScheduler(); release=threading.Event(); started=[]; lock=threading.Lock()
    for i in range(8):
        def _r(_w,i=i):
            with lock: started.append(i)
            release.wait(3)
        assert s.enqueue(_task(f"M{i}",run=_r))
    assert _wait_until(lambda:len(started)==8,2), s.snapshot()
    for i in range(8,24): assert s.enqueue(_task(f"M{i}"))
    s.stop_after_drain(); release.set(); assert s.quiesce(5), s.snapshot()
    assert not any(t.is_alive() for t in s._threads)
    snap=s.snapshot(); assert snap["completed"]==24, snap
    assert snap["task_conservation_delta"]==0 and snap["task_conservation_valid"] is True, snap
