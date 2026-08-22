# -*- coding: utf-8 -*-
"""Bounded fair downstream scheduler for SOURCE_PRIORITY observations.

H12H14H8 protected-source-capacity contract:
- MetaStock/source identity stays canonical and unchanged.
- The source coordinator performs bounded discovery/read/freshness work only.
- Historical callers remain serial by default. US local-DAT tasks may request a
  bounded key-serialized worker pool (16 by default) with protected reserve (8).
- H12H14H8 production runtime explicitly opts Saudi local-DAT into a bounded
  8-worker pool with protected reserve 4; legacy/direct callers remain serial unless
  they opt into the H8 Saudi-capacity contract.
- The same physical DAT path never overlaps; the same market+symbol never overlaps
  even when it is observed through different DAT paths. Different symbols may run
  concurrently.
- Neutral work cannot consume the protected reserve, so decision-bearing Cross/R1/
  Seal work cannot be trapped behind a neutral storm.
- Per-market concurrency fences are enforced from each task's contract: US <=16,
  Saudi H8 <=8, while FX remains serial. A larger US pool cannot inflate Saudi/FX.
- R1 / active-cross / near observations may jump ahead of general work, but after
  three hot tasks one runnable general task is serviced when available (no starvation).
- A bounded queue never drops an observation: admission failure returns ownership
  to the physical-generation retry path.
"""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
import os
import threading
import time
from typing import Any, Callable, Deque, Dict, Optional

from exception_observability import report_suppressed_exception

VERSION = "A4_2_14_CORE_CAUSAL_TRUTH_HOTFIX12H14H5_US_PROTECTED_SOURCE_CAPACITY_HOTFIX12H14H7_SAUDI_PROTECTED_SOURCE_CAPACITY_HOTFIX12H14H8_V1"
_SCHED_ATTR = "_live_source_processing_scheduler"
_CREATE_LOCK = threading.RLock()


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    try:
        return max(lo, min(hi, int(os.getenv(name, str(default)) or default)))
    except Exception:
        return default


def source_processing_capacity_hints(
    market: str, norm: str, source_kind: str = "", *, saudi_capacity_enabled: bool = False
) -> tuple[int, int]:
    """Return (parallelism, protected_reserve) for one physical source task.

    H12H14H7 remains backward-compatible: US local MetaStock/DAT is 16/8 while
    historical/direct Saudi callers remain 1/0. H12H14H8 production runtime opts
    Saudi local MetaStock/DAT into 8/4 explicitly via ``saudi_capacity_enabled``.
    API sources and FX retain the historical serial contract.
    """
    market_text = str(market or "").strip().lower()
    norm_text = str(norm or "").strip().lower()
    source_text = str(source_kind or "").strip().lower()
    us_local = bool(
        market_text.startswith("local_")
        and ("الأمريك" in market_text or "american" in market_text or market_text in {"local_us", "us_local"})
    )
    saudi_local = bool(
        market_text.startswith("local_")
        and ("السعود" in market_text or "saudi" in market_text or market_text in {"local_sa", "sa_local"})
    )
    local_dat = bool(norm_text.endswith(".dat"))
    api_source = bool(market_text.startswith("api_") or "api" in source_text)
    if us_local and local_dat and not api_source:
        workers = _env_int("AIN_LIVE_SOURCE_US_DAT_WORKERS", 16, 1, 32)
        reserve_hi = max(0, workers - 1)
        reserve = _env_int("AIN_LIVE_SOURCE_US_DAT_PROTECTED_RESERVE", 8, 0, reserve_hi)
        return int(workers), int(reserve)
    if bool(saudi_capacity_enabled) and saudi_local and local_dat and not api_source:
        workers = _env_int("AIN_LIVE_SOURCE_SAUDI_DAT_WORKERS", 8, 1, 16)
        reserve_hi = max(0, workers - 1)
        reserve = _env_int("AIN_LIVE_SOURCE_SAUDI_DAT_PROTECTED_RESERVE", 4, 0, reserve_hi)
        return int(workers), int(reserve)
    return 1, 0


@dataclass
class SourceProcessingTask:
    market: str
    norm: str
    bucket: int
    enqueued_monotonic: float
    run: Callable[[float], Any]
    cancel: Callable[[str], Any]
    # Physical source generation when known (MetaStock DAT mtime_ns).  -1 keeps
    # compatibility with synthetic/tests and falls back to enqueue order.
    generation_seq: int = -1
    # Only neutral/no-active-episode generations may be superseded. Decision-bearing
    # work defaults fail-safe to non-coalescible so a transient cross/R1/SEAL fact
    # can never disappear merely because a newer DAT generation arrived.
    coalescible: bool = False
    # H12H14H4: explicit production proof that this task is neutral even when a
    # decision barrier for the same DAT is active. Historical callers/tests leave
    # this False and retain the stricter H12H14 behaviour. When True, only the
    # newest neutral generation is retained *behind* the protected barrier; it can
    # never supersede or run ahead of decision-bearing work.
    barrier_coalescible: bool = False
    # H12H14H5 semantic generation. Appended after legacy positional fields so
    # historical SourceProcessingTask constructors retain their exact meaning.
    generation_identity: str = ""
    # H12H14H7 serialization identity and bounded capacity hints. Historical
    # constructors omit these fields and therefore retain the serial worker contract.
    symbol: str = ""
    parallelism_hint: int = 1
    protected_reserve_hint: int = 0


class SourceProcessingScheduler:
    def __init__(self) -> None:
        self._cv = threading.Condition(threading.RLock())
        self._queues: Dict[int, Deque[SourceProcessingTask]] = {0: deque(), 1: deque(), 2: deque(), 3: deque()}
        self._max_pending = _env_int("AIN_LIVE_SOURCE_PROCESS_QUEUE_MAX", 1024, 32, 8192)
        # _thread is retained as a compatibility alias for historical courts.
        self._thread: Optional[threading.Thread] = None
        self._threads: list[threading.Thread] = []
        self._worker_target = 1
        self._global_protected_reserve = 0
        self._stopping = False
        self._pending_by_key: Dict[tuple[str, str], SourceProcessingTask] = {}
        # Latest verified-neutral task held behind an active decision barrier. It
        # owns its source lease, is counted as pending, and is promoted only after
        # the last protected task for that physical source completes.
        self._barrier_neutral_by_key: Dict[tuple[str, str], SourceProcessingTask] = {}
        self._barrier_counts: Dict[tuple[str, str], int] = {}
        self._hot_streak = 0
        self._enqueue_counter = 0
        # H7 worker claims are made while holding _cv, before the task leaves the
        # queue, so two workers can never acquire the same DAT or symbol concurrently.
        self._inflight_dat_keys: set[tuple[str, str]] = set()
        self._inflight_symbol_keys: set[tuple[str, str]] = set()
        self._market_inflight: Dict[str, int] = {}
        self._market_neutral_inflight: Dict[str, int] = {}
        self._neutral_inflight_total = 0
        self._market_parallelism_seen: Dict[str, int] = {}
        self._market_reserve_seen: Dict[str, int] = {}
        self._stats: Dict[str, float] = {
            "enqueued": 0, "completed": 0, "failed": 0, "cancelled": 0,
            "queue_full": 0, "superseded_pending": 0, "superseded_incoming": 0,
            "decision_barrier_enqueued": 0, "decision_barrier_forced": 0,
            "barrier_neutral_deferred": 0, "barrier_neutral_promoted": 0,
            "barrier_neutral_superseded": 0, "barrier_neutral_dominated": 0,
            "protected_overflow_admitted": 0,
            "protected_completed": 0, "protected_failed": 0, "protected_cancelled": 0,
            "cancel_callback_failed": 0,
            "inflight": 0, "inflight_max": 0,
            "queue_wait_ms_total": 0.0, "queue_wait_ms_max": 0.0,
            "protected_queue_wait_ms_max": 0.0,
            "hot_queue_wait_ms_max": 0.0,
            "processing_ms_total": 0.0, "processing_ms_max": 0.0,
            "worker_count_peak": 0,
        }

    def _pending_locked(self) -> int:
        return sum(len(q) for q in self._queues.values()) + len(self._barrier_neutral_by_key)

    @staticmethod
    def _task_parallelism(task: SourceProcessingTask) -> int:
        try:
            return max(1, min(32, int(getattr(task, "parallelism_hint", 1) or 1)))
        except Exception:
            return 1

    @classmethod
    def _task_reserve(cls, task: SourceProcessingTask) -> int:
        parallelism = cls._task_parallelism(task)
        try:
            return max(0, min(max(0, parallelism - 1), int(getattr(task, "protected_reserve_hint", 0) or 0)))
        except Exception:
            return 0

    @staticmethod
    def _task_symbol(task: SourceProcessingTask) -> str:
        return str(getattr(task, "symbol", "") or "").strip().upper()

    def _ensure_thread_locked(self, requested_parallelism: int = 1, requested_reserve: int = 0) -> None:
        if self._stopping:
            return
        requested_parallelism = max(1, min(32, int(requested_parallelism or 1)))
        requested_reserve = max(0, min(requested_parallelism - 1, int(requested_reserve or 0)))
        self._worker_target = max(int(self._worker_target or 1), requested_parallelism)
        self._global_protected_reserve = max(int(self._global_protected_reserve or 0), requested_reserve)
        self._global_protected_reserve = min(self._global_protected_reserve, self._worker_target - 1)
        self._threads = [t for t in self._threads if t.is_alive()]
        while len(self._threads) < self._worker_target:
            index = len(self._threads) + 1
            thread = threading.Thread(
                target=self._worker, daemon=True, name=f"AinSourceTruthProcessor-{index:02d}"
            )
            self._threads.append(thread)
            if self._thread is None or not self._thread.is_alive():
                self._thread = thread
            thread.start()
        self._stats["worker_count_peak"] = max(
            int(self._stats.get("worker_count_peak", 0) or 0), len(self._threads)
        )

    def enqueue(self, task: SourceProcessingTask) -> bool:
        bucket = max(0, min(3, int(task.bucket)))
        task.bucket = bucket
        superseded: Optional[SourceProcessingTask] = None
        dominated_incoming: Optional[SourceProcessingTask] = None
        key = (str(task.market or ""), str(task.norm or ""))
        task_parallelism = self._task_parallelism(task)
        task_reserve = self._task_reserve(task)
        setattr(task, "parallelism_hint", task_parallelism)
        setattr(task, "protected_reserve_hint", task_reserve)
        setattr(task, "symbol", self._task_symbol(task))
        with self._cv:
            self._enqueue_counter += 1
            setattr(task, "_scheduler_seq", int(self._enqueue_counter))
            self._market_parallelism_seen[key[0]] = max(
                int(self._market_parallelism_seen.get(key[0], 1) or 1), task_parallelism
            )
            self._market_reserve_seen[key[0]] = max(
                int(self._market_reserve_seen.get(key[0], 0) or 0), task_reserve
            )
            if self._stopping:
                self._stats["queue_full"] += 1
                return False

            requested_coalescible = bool(getattr(task, "coalescible", False))
            barrier_active = int(self._barrier_counts.get(key, 0) or 0) > 0
            barrier_safe_neutral = bool(
                requested_coalescible
                and barrier_active
                and bool(getattr(task, "barrier_coalescible", False))
            )

            # H12H14H4: a *verified-neutral* rewrite that arrives behind protected
            # decision truth is not promoted to another protected task. Keep exactly
            # one newest neutral generation off-queue until the barrier drains. This
            # preserves the transient decision fact and the latest post-decision DAT
            # state without turning a rewrite storm into an unbounded barrier chain.
            if barrier_safe_neutral:
                setattr(task, "_effective_coalescible", True)
                setattr(task, "_source_barrier_key", key)
                setattr(task, "_barrier_deferred_neutral", True)
                previous = self._barrier_neutral_by_key.get(key)
                if previous is not None:
                    prev_gen = int(getattr(previous, "generation_seq", -1) or -1)
                    new_gen = int(getattr(task, "generation_seq", -1) or -1)
                    prev_identity = str(getattr(previous, "generation_identity", "") or "")
                    new_identity = str(getattr(task, "generation_identity", "") or "")
                    same_or_legacy_identity = bool((not prev_identity and not new_identity) or (prev_identity and prev_identity == new_identity))
                    if prev_gen >= 0 and new_gen >= 0 and (new_gen < prev_gen or (new_gen == prev_gen and same_or_legacy_identity)):
                        dominated_incoming = task
                        self._stats["superseded_incoming"] += 1
                        self._stats["barrier_neutral_dominated"] += 1
                    else:
                        superseded = previous
                        self._barrier_neutral_by_key[key] = task
                        self._stats["superseded_pending"] += 1
                        self._stats["barrier_neutral_superseded"] += 1
                else:
                    if self._pending_locked() >= self._max_pending:
                        # The deferred neutral is still neutral. Never turn it into
                        # protected overflow; return ownership to the physical pending
                        # map where latest-generation-wins remains safe.
                        self._stats["queue_full"] += 1
                        return False
                    self._barrier_neutral_by_key[key] = task
                    self._stats["barrier_neutral_deferred"] += 1
                if dominated_incoming is None:
                    self._stats["enqueued"] += 1
                    self._ensure_thread_locked(task_parallelism, task_reserve)
                    self._cv.notify_all()
            else:
                # Legacy H12H14 semantics remain unchanged unless the runtime provides
                # the explicit H4 neutrality proof above. This keeps all historical
                # no-loss courts intact.
                effective_coalescible = bool(requested_coalescible and not barrier_active)
                setattr(task, "_effective_coalescible", effective_coalescible)
                setattr(task, "_source_barrier_key", key)

                if effective_coalescible:
                    previous = self._pending_by_key.get(key)
                    if previous is not None:
                        prev_gen = int(getattr(previous, "generation_seq", -1) or -1)
                        new_gen = int(getattr(task, "generation_seq", -1) or -1)
                        prev_identity = str(getattr(previous, "generation_identity", "") or "")
                        new_identity = str(getattr(task, "generation_identity", "") or "")
                        same_or_legacy_identity = bool((not prev_identity and not new_identity) or (prev_identity and prev_identity == new_identity))
                        if prev_gen >= 0 and new_gen >= 0 and (new_gen < prev_gen or (new_gen == prev_gen and same_or_legacy_identity)):
                            dominated_incoming = task
                            self._stats["superseded_incoming"] += 1
                        else:
                            try:
                                self._queues[int(previous.bucket)].remove(previous)
                                superseded = previous
                                self._stats["superseded_pending"] += 1
                            except ValueError:
                                superseded = None
                    if dominated_incoming is None:
                        if superseded is None and self._pending_locked() >= self._max_pending:
                            self._stats["queue_full"] += 1
                            return False
                        self._queues[bucket].append(task)
                        self._pending_by_key[key] = task
                        self._stats["enqueued"] += 1
                        self._ensure_thread_locked(task_parallelism, task_reserve)
                        self._cv.notify()
                else:
                    # A newer protected generation may supersede an older neutral
                    # queued/deferred generation, but never another protected fact.
                    previous = self._pending_by_key.get(key)
                    if previous is not None and bool(getattr(previous, "_effective_coalescible", False)):
                        prev_gen = int(getattr(previous, "generation_seq", -1) or -1)
                        new_gen = int(getattr(task, "generation_seq", -1) or -1)
                        if prev_gen >= 0 and new_gen >= 0 and new_gen >= prev_gen:
                            try:
                                self._queues[int(previous.bucket)].remove(previous)
                                self._pending_by_key.pop(key, None)
                                superseded = previous
                                self._stats["superseded_pending"] += 1
                            except ValueError:
                                superseded = None
                    deferred = self._barrier_neutral_by_key.get(key)
                    if deferred is not None:
                        prev_gen = int(getattr(deferred, "generation_seq", -1) or -1)
                        new_gen = int(getattr(task, "generation_seq", -1) or -1)
                        if prev_gen >= 0 and new_gen >= 0 and new_gen >= prev_gen:
                            self._barrier_neutral_by_key.pop(key, None)
                            # Prefer cancelling the deferred neutral over an older
                            # queued neutral if both somehow exist for the same key.
                            if superseded is not None:
                                try:
                                    superseded.cancel("SUPERSEDED_BY_NEWER_SOURCE_TASK")
                                except Exception as exc:
                                    self._stats["cancel_callback_failed"] = int(self._stats.get("cancel_callback_failed", 0) or 0) + 1
                                    report_suppressed_exception(
                                        exc, module=__name__, file=__file__, function="enqueue",
                                        stage="source_parse", critical=True,
                                        reason_code="SOURCE_PROCESSING_SUPERSEDE_CALLBACK_FAILED",
                                    )
                            superseded = deferred
                            self._stats["superseded_pending"] += 1
                            self._stats["barrier_neutral_superseded"] += 1
                    if self._pending_locked() >= self._max_pending:
                        self._stats["queue_full"] += 1
                        self._stats["protected_overflow_admitted"] += 1
                    self._queues[bucket].append(task)
                    self._barrier_counts[key] = int(self._barrier_counts.get(key, 0) or 0) + 1
                    setattr(task, "_owns_source_barrier", True)
                    self._stats["decision_barrier_enqueued"] += 1
                    if requested_coalescible and barrier_active:
                        self._stats["decision_barrier_forced"] += 1
                    self._stats["enqueued"] += 1
                    self._ensure_thread_locked(task_parallelism, task_reserve)
                    self._cv.notify()

        to_cancel = dominated_incoming or superseded
        if to_cancel is not None:
            try:
                to_cancel.cancel("SUPERSEDED_BY_NEWER_SOURCE_TASK")
            except Exception as exc:
                with self._cv:
                    self._stats["cancel_callback_failed"] = int(self._stats.get("cancel_callback_failed", 0) or 0) + 1
                report_suppressed_exception(
                    exc, module=__name__, file=__file__, function="enqueue",
                    stage="source_parse", critical=True,
                    reason_code="SOURCE_PROCESSING_SUPERSEDE_CALLBACK_FAILED",
                )
        return True

    def _release_source_barrier_locked(self, task: SourceProcessingTask, *, promote_deferred: bool = True) -> None:
        if not bool(getattr(task, "_owns_source_barrier", False)):
            return
        key = getattr(task, "_source_barrier_key", None)
        if not isinstance(key, tuple) or len(key) != 2:
            key = (str(task.market or ""), str(task.norm or ""))
        remaining = max(0, int(self._barrier_counts.get(key, 0) or 0) - 1)
        if remaining:
            self._barrier_counts[key] = remaining
        else:
            self._barrier_counts.pop(key, None)
            if promote_deferred:
                deferred = self._barrier_neutral_by_key.pop(key, None)
                if deferred is not None:
                    setattr(deferred, "_barrier_deferred_neutral", False)
                    self._queues[int(deferred.bucket)].append(deferred)
                    self._pending_by_key[key] = deferred
                    self._stats["barrier_neutral_promoted"] += 1
        setattr(task, "_owns_source_barrier", False)

    def _same_serialization_key(self, left: SourceProcessingTask, right: SourceProcessingTask) -> bool:
        if str(left.market or "") != str(right.market or ""):
            return False
        if str(left.norm or "") == str(right.norm or ""):
            return True
        ls = self._task_symbol(left)
        rs = self._task_symbol(right)
        return bool(ls and rs and ls == rs)

    def _has_earlier_same_key_locked(self, task: SourceProcessingTask) -> bool:
        seq = int(getattr(task, "_scheduler_seq", 0) or 0)
        if seq <= 0:
            return False
        for q in self._queues.values():
            for other in q:
                if other is task:
                    continue
                other_seq = int(getattr(other, "_scheduler_seq", 0) or 0)
                if 0 < other_seq < seq and self._same_serialization_key(other, task):
                    return True
        return False

    def _can_claim_locked(self, task: SourceProcessingTask) -> bool:
        market = str(task.market or "")
        dat_key = (market, str(task.norm or ""))
        symbol = self._task_symbol(task)
        symbol_key = (market, symbol) if symbol else None
        if dat_key in self._inflight_dat_keys:
            return False
        if symbol_key is not None and symbol_key in self._inflight_symbol_keys:
            return False
        parallelism = self._task_parallelism(task)
        if int(self._market_inflight.get(market, 0) or 0) >= parallelism:
            return False
        neutral = bool(getattr(task, "_effective_coalescible", False))
        if neutral:
            reserve = self._task_reserve(task)
            market_neutral_cap = max(1, parallelism - reserve)
            if int(self._market_neutral_inflight.get(market, 0) or 0) >= market_neutral_cap:
                return False
            global_neutral_cap = max(1, int(self._worker_target or 1) - int(self._global_protected_reserve or 0))
            if int(self._neutral_inflight_total or 0) >= global_neutral_cap:
                return False
        return True

    def _claim_locked(self, task: SourceProcessingTask) -> None:
        market = str(task.market or "")
        dat_key = (market, str(task.norm or ""))
        symbol = self._task_symbol(task)
        self._inflight_dat_keys.add(dat_key)
        if symbol:
            self._inflight_symbol_keys.add((market, symbol))
        self._market_inflight[market] = int(self._market_inflight.get(market, 0) or 0) + 1
        if bool(getattr(task, "_effective_coalescible", False)):
            self._neutral_inflight_total += 1
            self._market_neutral_inflight[market] = int(self._market_neutral_inflight.get(market, 0) or 0) + 1
        setattr(task, "_worker_claimed", True)

    def _release_worker_claim_locked(self, task: SourceProcessingTask) -> None:
        if not bool(getattr(task, "_worker_claimed", False)):
            return
        market = str(task.market or "")
        dat_key = (market, str(task.norm or ""))
        symbol = self._task_symbol(task)
        self._inflight_dat_keys.discard(dat_key)
        if symbol:
            self._inflight_symbol_keys.discard((market, symbol))
        remaining = max(0, int(self._market_inflight.get(market, 0) or 0) - 1)
        if remaining:
            self._market_inflight[market] = remaining
        else:
            self._market_inflight.pop(market, None)
        if bool(getattr(task, "_effective_coalescible", False)):
            self._neutral_inflight_total = max(0, int(self._neutral_inflight_total or 0) - 1)
            neutral_remaining = max(0, int(self._market_neutral_inflight.get(market, 0) or 0) - 1)
            if neutral_remaining:
                self._market_neutral_inflight[market] = neutral_remaining
            else:
                self._market_neutral_inflight.pop(market, None)
        setattr(task, "_worker_claimed", False)

    def _first_runnable_locked(self, bucket: int) -> Optional[SourceProcessingTask]:
        for task in tuple(self._queues[int(bucket)]):
            if self._can_claim_locked(task):
                return task
        return None

    def _take_locked(self, task: SourceProcessingTask, *, claim_worker: bool = False) -> SourceProcessingTask:
        self._queues[int(task.bucket)].remove(task)
        key = (str(task.market or ""), str(task.norm or ""))
        if self._pending_by_key.get(key) is task:
            self._pending_by_key.pop(key, None)
        # Historical H12H11 courts call _choose_locked() directly as a pure
        # priority-selection primitive.  Worker claims are therefore opt-in and
        # are taken only by the real worker path while still holding _cv.
        if claim_worker:
            self._claim_locked(task)
        return task

    def _choose_locked(self, *, claim_worker: bool = False) -> Optional[SourceProcessingTask]:
        general = self._first_runnable_locked(3)
        hot = None
        for i in (0, 1, 2):
            hot = self._first_runnable_locked(i)
            if hot is not None:
                break
        if general is not None and self._hot_streak >= 3:
            self._hot_streak = 0
            return self._take_locked(general, claim_worker=claim_worker)
        if hot is not None:
            self._hot_streak += 1
            return self._take_locked(hot, claim_worker=claim_worker)
        if general is not None:
            self._hot_streak = 0
            return self._take_locked(general, claim_worker=claim_worker)
        return None

    def _worker(self) -> None:
        while True:
            with self._cv:
                while not self._stopping and self._pending_locked() <= 0:
                    self._cv.wait(timeout=0.5)
                if self._stopping and self._pending_locked() <= 0:
                    return
                task = self._choose_locked(claim_worker=True)
                if task is None:
                    # Pending work may be key-blocked or reserve-blocked by another
                    # worker. Sleep on the condition rather than spin; completion or
                    # cancellation will notify all workers.
                    self._cv.wait(timeout=0.05)
                    continue
            wait_ms = max(0.0, (time.monotonic() - float(task.enqueued_monotonic)) * 1000.0)
            started = time.monotonic()
            with self._cv:
                self._stats["inflight"] += 1
                self._stats["inflight_max"] = max(self._stats["inflight_max"], self._stats["inflight"])
                self._stats["queue_wait_ms_total"] += wait_ms
                self._stats["queue_wait_ms_max"] = max(self._stats["queue_wait_ms_max"], wait_ms)
                if not bool(getattr(task, "_effective_coalescible", False)):
                    self._stats["protected_queue_wait_ms_max"] = max(
                        self._stats.get("protected_queue_wait_ms_max", 0.0), wait_ms
                    )
                # H12H14H7 observability: bucket 0 is a valid hot bucket.  The
                # historical ``... or 3`` expression converted integer zero to the
                # general bucket and could report a false 0 ms hot-queue maximum.
                try:
                    _metric_bucket = int(getattr(task, "bucket", 3))
                except Exception:
                    _metric_bucket = 3
                if _metric_bucket <= 2:
                    self._stats["hot_queue_wait_ms_max"] = max(
                        self._stats.get("hot_queue_wait_ms_max", 0.0), wait_ms
                    )
            try:
                task.run(wait_ms)
                with self._cv:
                    self._stats["completed"] += 1
                    if not bool(getattr(task, "_effective_coalescible", False)):
                        self._stats["protected_completed"] += 1
            except Exception:
                with self._cv:
                    self._stats["failed"] += 1
                    if not bool(getattr(task, "_effective_coalescible", False)):
                        self._stats["protected_failed"] += 1
            finally:
                processing_ms = max(0.0, (time.monotonic() - started) * 1000.0)
                with self._cv:
                    self._stats["inflight"] = max(0, int(self._stats.get("inflight", 0) or 0) - 1)
                    self._stats["processing_ms_total"] += processing_ms
                    self._stats["processing_ms_max"] = max(self._stats["processing_ms_max"], processing_ms)
                    self._release_source_barrier_locked(task)
                    self._release_worker_claim_locked(task)
                    self._cv.notify_all()

    def cancel_market(self, market: str, reason: str = "MARKET_CANCELLED") -> int:
        key = str(market or "")
        cancelled = []
        with self._cv:
            for bucket in self._queues:
                keep = deque()
                while self._queues[bucket]:
                    task = self._queues[bucket].popleft()
                    if str(task.market or "") == key:
                        cancelled.append(task)
                    else:
                        keep.append(task)
                self._queues[bucket] = keep
            # Do not promote deferred neutrals while cancelling this market.
            for task in cancelled:
                self._release_source_barrier_locked(task, promote_deferred=False)
            for dkey, task in list(self._barrier_neutral_by_key.items()):
                if dkey[0] == key:
                    cancelled.append(task)
                    self._barrier_neutral_by_key.pop(dkey, None)
            self._pending_by_key = {
                (str(task.market or ""), str(task.norm or "")): task
                for q in self._queues.values() for task in q
                if bool(getattr(task, "_effective_coalescible", False))
            }
            self._stats["cancelled"] += len(cancelled)
            self._stats["protected_cancelled"] += sum(
                1 for task in cancelled if not bool(getattr(task, "_effective_coalescible", False))
            )
            self._cv.notify_all()
        for task in cancelled:
            try:
                task.cancel(reason)
            except Exception as exc:
                with self._cv:
                    self._stats["cancel_callback_failed"] = int(self._stats.get("cancel_callback_failed", 0) or 0) + 1
                report_suppressed_exception(
                    exc, module=__name__, file=__file__, function="cancel_market",
                    stage="source_parse", critical=True,
                    reason_code="SOURCE_PROCESSING_CANCEL_CALLBACK_FAILED",
                )
        return len(cancelled)

    def stop_after_drain(self) -> None:
        """Fence new work but preserve every already-admitted task until terminal.

        Formal frozen-sample shutdown must define a causal cohort boundary: first
        stop the physical producers, then drain all source work that was already
        admitted before that boundary. ``cancel_all`` remains available for
        ordinary emergency/legacy shutdown, but it is not a valid research-sample
        boundary because queued neutral observations are part of the cohort too.
        """
        with self._cv:
            self._stopping = True
            self._cv.notify_all()

    def cancel_all(self, reason: str = "SHUTDOWN") -> int:
        cancelled = []
        with self._cv:
            self._stopping = True
            for bucket in self._queues:
                while self._queues[bucket]:
                    cancelled.append(self._queues[bucket].popleft())
            for task in cancelled:
                self._release_source_barrier_locked(task, promote_deferred=False)
            self._pending_by_key.clear()
            if self._barrier_neutral_by_key:
                cancelled.extend(self._barrier_neutral_by_key.values())
                self._barrier_neutral_by_key.clear()
            # Release barriers only for queued tasks above.  An in-flight
            # protected task still owns its source barrier until its finally
            # block completes; clearing the whole map here would make shutdown
            # telemetry claim the decision barrier drained while authority work
            # is still executing.
            self._stats["cancelled"] += len(cancelled)
            self._stats["protected_cancelled"] += sum(
                1 for task in cancelled if not bool(getattr(task, "_effective_coalescible", False))
            )
            self._cv.notify_all()
        for task in cancelled:
            try:
                task.cancel(reason)
            except Exception as exc:
                with self._cv:
                    self._stats["cancel_callback_failed"] = int(self._stats.get("cancel_callback_failed", 0) or 0) + 1
                report_suppressed_exception(
                    exc, module=__name__, file=__file__, function="cancel_all",
                    stage="source_parse", critical=True,
                    reason_code="SOURCE_PROCESSING_CANCEL_CALLBACK_FAILED",
                )
        return len(cancelled)

    def quiesce(self, timeout: float = 5.0) -> bool:
        """Wait for the already-stopped scheduler's in-flight authority task.

        ``cancel_all`` removes queued work but cannot preempt a task that already
        owns source truth.  Frozen-sample shutdown must wait for that task instead
        of taking a health snapshot while it is still mutating durable state.
        """
        deadline = time.monotonic() + max(0.1, float(timeout))
        with self._cv:
            while True:
                pending = self._pending_locked()
                inflight = int(self._stats.get("inflight", 0) or 0)
                barriers = sum(int(v or 0) for v in self._barrier_counts.values())
                if pending == 0 and inflight == 0 and barriers == 0:
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cv.wait(timeout=min(0.10, remaining))
        for thread in list(self._threads):
            if thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=max(0.0, deadline - time.monotonic()))
        with self._cv:
            return bool(
                self._pending_locked() == 0
                and int(self._stats.get("inflight", 0) or 0) == 0
                and sum(int(v or 0) for v in self._barrier_counts.values()) == 0
                and not any(t.is_alive() for t in self._threads)
            )

    def snapshot(self) -> Dict[str, Any]:
        with self._cv:
            pending_by_bucket = {str(k): len(v) for k, v in self._queues.items()}
            completed = int(self._stats.get("completed", 0) or 0)
            failed = int(self._stats.get("failed", 0) or 0)
            inflight = int(self._stats.get("inflight", 0) or 0)
            pending = self._pending_locked()
            cancelled = int(self._stats.get("cancelled", 0) or 0)
            superseded_pending = int(self._stats.get("superseded_pending", 0) or 0)
            enqueued = int(self._stats.get("enqueued", 0) or 0)
            started = max(0, completed + failed + inflight)
            conservation_delta = enqueued - (completed + failed + cancelled + superseded_pending + pending + inflight)
            return {
                **dict(self._stats),
                "task_conservation_delta": int(conservation_delta),
                "task_conservation_valid": bool(conservation_delta == 0),
                "pending": self._pending_locked(),
                "pending_by_bucket": pending_by_bucket,
                "decision_barrier_keys": len(self._barrier_counts),
                "decision_barrier_depth": sum(int(v or 0) for v in self._barrier_counts.values()),
                "queue_wait_ms_mean": (
                    float(self._stats.get("queue_wait_ms_total", 0.0) or 0.0) / started
                    if started > 0 else 0.0
                ),
                "processing_ms_mean": (
                    float(self._stats.get("processing_ms_total", 0.0) or 0.0) / max(1, completed + failed)
                    if (completed + failed) > 0 else 0.0
                ),
                "worker_target": int(self._worker_target),
                "worker_count": sum(1 for t in self._threads if t.is_alive()),
                "protected_reserve": int(self._global_protected_reserve),
                "neutral_inflight": int(self._neutral_inflight_total),
                "market_inflight": dict(self._market_inflight),
                "market_parallelism_seen": dict(self._market_parallelism_seen),
                "market_reserve_seen": dict(self._market_reserve_seen),
                "thread_alive": any(t.is_alive() for t in self._threads),
            }


def source_processing_scheduler_for_owner(owner: Any) -> SourceProcessingScheduler:
    current = getattr(owner, _SCHED_ATTR, None)
    if isinstance(current, SourceProcessingScheduler):
        alive = any(t.is_alive() for t in getattr(current, "_threads", []) or [])
        if not bool(getattr(current, "_stopping", False)):
            return current
        # monitor_stop is restartable in the same process. Never revive a stopped
        # scheduler in place while any old authority worker is still alive.
        if alive:
            return current
    with _CREATE_LOCK:
        current = getattr(owner, _SCHED_ATTR, None)
        if isinstance(current, SourceProcessingScheduler):
            alive = any(t.is_alive() for t in getattr(current, "_threads", []) or [])
            if not bool(getattr(current, "_stopping", False)):
                return current
            if alive:
                return current
        current = SourceProcessingScheduler()
        setattr(owner, _SCHED_ATTR, current)
        return current


def quiesce_owner_source_processing(owner: Any, timeout: float = 5.0) -> bool:
    scheduler = getattr(owner, _SCHED_ATTR, None)
    if isinstance(scheduler, SourceProcessingScheduler):
        return scheduler.quiesce(timeout=timeout)
    return True


def stop_owner_source_processing_after_drain(owner: Any) -> None:
    scheduler = getattr(owner, _SCHED_ATTR, None)
    if isinstance(scheduler, SourceProcessingScheduler):
        scheduler.stop_after_drain()


def source_processing_snapshot_for_owner(owner: Any) -> Dict[str, Any]:
    """Read scheduler telemetry without creating a processing authority worker."""
    scheduler = getattr(owner, _SCHED_ATTR, None)
    if isinstance(scheduler, SourceProcessingScheduler):
        return scheduler.snapshot()
    return {
        "enqueued": 0, "completed": 0, "failed": 0, "cancelled": 0,
        "queue_full": 0, "superseded_pending": 0, "superseded_incoming": 0,
        "decision_barrier_enqueued": 0, "decision_barrier_forced": 0,
        "barrier_neutral_deferred": 0, "barrier_neutral_promoted": 0,
        "barrier_neutral_superseded": 0, "barrier_neutral_dominated": 0,
        "decision_barrier_keys": 0, "decision_barrier_depth": 0,
        "protected_overflow_admitted": 0, "protected_completed": 0,
        "protected_failed": 0, "protected_cancelled": 0,
        "inflight": 0, "inflight_max": 0, "pending": 0,
        "pending_by_bucket": {"0": 0, "1": 0, "2": 0, "3": 0},
        "queue_wait_ms_mean": 0.0, "queue_wait_ms_max": 0.0,
        "protected_queue_wait_ms_max": 0.0, "hot_queue_wait_ms_max": 0.0,
        "processing_ms_mean": 0.0, "processing_ms_max": 0.0,
        "worker_target": 1, "worker_count": 0, "worker_count_peak": 0,
        "protected_reserve": 0, "neutral_inflight": 0,
        "market_inflight": {}, "market_parallelism_seen": {}, "market_reserve_seen": {},
        "thread_alive": False,
    }

def cancel_owner_source_processing(owner: Any, market: str, reason: str = "") -> int:
    scheduler = getattr(owner, _SCHED_ATTR, None)
    if isinstance(scheduler, SourceProcessingScheduler):
        return scheduler.cancel_market(market, reason or "MARKET_CANCELLED")
    return 0


def cancel_all_owner_source_processing(owner: Any, reason: str = "") -> int:
    scheduler = getattr(owner, _SCHED_ATTR, None)
    if isinstance(scheduler, SourceProcessingScheduler):
        return scheduler.cancel_all(reason or "SHUTDOWN")
    return 0


__all__ = [
    "VERSION", "SourceProcessingTask", "SourceProcessingScheduler", "source_processing_capacity_hints",
    "source_processing_scheduler_for_owner", "source_processing_snapshot_for_owner",
    "stop_owner_source_processing_after_drain",
    "cancel_owner_source_processing", "cancel_all_owner_source_processing",
]
