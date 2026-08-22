# -*- coding: utf-8 -*-
"""Persistent isolated GANN20 probability worker.

The live engine keeps source capture, PAAC, decision/context/event writes and Qt in
its own process. Only the two pure probability functions are executed here so a
hung model/feature build can be terminated without killing the application.

IPC deliberately uses local atomic pickle files. The payload is produced by the
same trusted application process and may contain pandas DataFrames; it is never a
network-facing interface.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import pickle
import signal
import subprocess
import sys
import time
import traceback
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, Tuple

from atomic_io_utils import (
    read_pickle_retry,
    replace_with_retry,
    write_json_atomic,
    write_pickle_atomic,
)
from probability_worker_progress import execute_with_progress, worker_low_priority_requested

POLL_SEC = 0.02
HEARTBEAT_SEC = 0.5
WATCHDOG_POLL_SEC = 0.20
SNAPSHOT_CACHE_MAX = max(1, min(16, int(os.environ.get("AIN_PROBABILITY_SNAPSHOT_CACHE_MAX", "4") or 4)))
_SNAPSHOT_CACHE: "OrderedDict[str, Any]" = OrderedDict()


class ProbabilitySnapshotCacheMiss(RuntimeError):
    pass


def _utc_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _process_create_time(pid: int) -> float:
    try:
        import psutil
        return float(psutil.Process(int(pid)).create_time())
    except Exception:
        return 0.0


def _process_is_same_and_alive(pid: int, expected_start_time: float = 0.0) -> bool:
    process_id = int(pid or 0)
    if process_id <= 0:
        return True
    try:
        import psutil
        process = psutil.Process(process_id)
        if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
            return False
        if float(expected_start_time or 0.0) > 0.0:
            return abs(float(process.create_time()) - float(expected_start_time)) < 0.01
        return True
    except Exception:
        try:
            os.kill(process_id, 0)
            return True
        except PermissionError:
            return True
        except OSError:
            return False


def _kill_process(pid: int, expected_start_time: float = 0.0) -> bool:
    process_id = int(pid or 0)
    if process_id <= 0:
        return True
    try:
        import psutil
        process = psutil.Process(process_id)
        if float(expected_start_time or 0.0) > 0.0:
            if abs(float(process.create_time()) - float(expected_start_time)) >= 0.01:
                return False
        process.kill()
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if not _process_is_same_and_alive(process_id, expected_start_time):
                return True
            time.sleep(0.02)
        return not _process_is_same_and_alive(process_id, expected_start_time)
    except psutil.NoSuchProcess:
        return True
    except Exception:
        try:
            hard_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
            os.kill(process_id, hard_signal)
            return True
        except ProcessLookupError:
            return True
        except OSError:
            return False


def watch_parent(
    *,
    parent_pid: int,
    parent_start_time: float,
    worker_pid: int,
    worker_start_time: float,
    poll_sec: float = WATCHDOG_POLL_SEC,
) -> int:
    """Independent watchdog that can kill a scorer hung in native/Python code.

    A PID heartbeat in the scorer cannot run while the scorer itself is hung. This
    tiny second process watches the application and kills the scorer if the exact
    parent process disappears. Process creation times prevent accidental PID-reuse
    kills. It exits as soon as either process is gone.
    """
    delay = max(0.05, min(1.0, float(poll_sec)))
    while True:
        if not _process_is_same_and_alive(worker_pid, worker_start_time):
            return 0
        if not _process_is_same_and_alive(parent_pid, parent_start_time):
            return 0 if _kill_process(worker_pid, worker_start_time) else 2
        time.sleep(delay)


def _atomic_pickle(path: Path, payload: Any) -> None:
    write_pickle_atomic(path, payload)


def _atomic_json(path: Path, payload: Dict[str, Any]) -> None:
    write_json_atomic(path, payload, trailing_newline=True)


def _redirect_worker_log(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / f"probability_worker_{os.getpid()}.log"
    stream = path.open("a", encoding="utf-8", buffering=1)
    sys.stdout = stream
    sys.stderr = stream
    return path


def _load_request(path: Path) -> Dict[str, Any]:
    payload = read_pickle_retry(path)
    if not isinstance(payload, dict):
        raise TypeError("PROBABILITY_WORKER_REQUEST_NOT_DICT")
    return payload


def _snapshot_for_request(payload: Dict[str, Any]) -> Tuple[Any, Dict[str, Any]]:
    key = str(payload.get("snapshot_key") or "").strip()
    supplied = payload.get("market_df")
    started = time.perf_counter()
    cache_hit = False
    if supplied is not None:
        frame = supplied
        if key:
            _SNAPSHOT_CACHE[key] = frame
            _SNAPSHOT_CACHE.move_to_end(key)
            while len(_SNAPSHOT_CACHE) > SNAPSHOT_CACHE_MAX:
                _SNAPSHOT_CACHE.popitem(last=False)
    elif key and key in _SNAPSHOT_CACHE:
        frame = _SNAPSHOT_CACHE[key]
        _SNAPSHOT_CACHE.move_to_end(key)
        cache_hit = True
    else:
        raise ProbabilitySnapshotCacheMiss(f"PROBABILITY_SNAPSHOT_CACHE_MISS:{key or 'MISSING_KEY'}")
    return frame, {
        "snapshot_cache_key": key,
        "snapshot_cache_hit": bool(cache_hit),
        "snapshot_fetch_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }


def _timing_from_result(result: Any) -> Dict[str, Any]:
    rows = []
    if isinstance(result, dict):
        if result and all(isinstance(value, dict) for value in result.values()):
            rows = [dict(value or {}) for value in result.values()]
        else:
            rows = [dict(result)]
    if not rows:
        return {}
    def _max(name: str) -> float:
        values = []
        for row in rows:
            try:
                value = float(row.get(name) or 0.0)
                if value >= 0.0:
                    values.append(value)
            except (TypeError, ValueError, OverflowError):
                continue
        return max(values) if values else 0.0
    return {
        "feature_build_ms": round(_max("feature_build_ms"), 3),
        "model_load_ms": round(_max("model_load_ms"), 3),
        "model_score_ms": round(_max("model_score_ms"), 3),
    }


def _base_execution_meta(payload: Dict[str, Any]) -> Dict[str, Any]:
    now_epoch = _dt.datetime.now(_dt.timezone.utc).timestamp(); requested_at = float(payload.get("requested_at_epoch") or now_epoch)
    return {
        "worker_started_at": _utc_now(),
        "worker_queue_ms": round(max(0.0, now_epoch - requested_at) * 1000.0, 3),
        "snapshot_cache_hit": False,
        "snapshot_cache_key": str(payload.get("snapshot_key") or ""),
        "snapshot_fetch_ms": 0.0,
    }


def _hot_path_enabled() -> bool:
    return str(os.environ.get("AIN_PROBABILITY_HOT_PATH", "1") or "1").strip().lower() not in {
        "0", "false", "no", "off",
    }


def _fallback_meta(exc: BaseException) -> Dict[str, Any]:
    return {
        "probability_hot_path": False,
        "probability_hot_path_fallback": True,
        "probability_hot_path_error_type": type(exc).__name__,
        "probability_hot_path_error_message": str(exc),
    }


def _score_batch_request(payload: Dict[str, Any], meta: Dict[str, Any], progress: Any) -> Any:
    from live_pulse_seal_engine import consume_probability_stage_timing, estimate_market_live_probability_batch
    if progress is not None:
        progress("SNAPSHOT_FETCH", {"mode": "batch"})
    market_df, snapshot_meta = _snapshot_for_request(payload)
    meta.update(snapshot_meta)
    try:
        if not _hot_path_enabled():
            raise RuntimeError("A98_HOT_PATH_DISABLED")
        from probability_hot_path import score_batch
        result, hot_meta = score_batch(
            market_df, payload.get("records_by_symbol") or {},
            market_key=str(payload.get("market_key") or ""),
            snapshot_key=str(payload.get("snapshot_key") or ""), progress=progress,
        )
        meta.update(dict(hot_meta or {})); meta["probability_hot_path"] = True
        return result
    except Exception as exc:
        if progress is not None:
            progress("HOT_PATH_FALLBACK", {"error_type": type(exc).__name__, "error_message": str(exc)})
        result = estimate_market_live_probability_batch(
            market_df, payload.get("records_by_symbol") or {},
            market_key=str(payload.get("market_key") or ""),
        )
        meta.update(consume_probability_stage_timing() or _timing_from_result(result)); meta.update(_fallback_meta(exc))
        return result


def _score_single_request(payload: Dict[str, Any], meta: Dict[str, Any], progress: Any) -> Any:
    from live_pulse_seal_engine import consume_probability_stage_timing, estimate_market_live_probability
    if progress is not None:
        progress("SNAPSHOT_FETCH", {"mode": "single"})
    market_df, snapshot_meta = _snapshot_for_request(payload)
    meta.update(snapshot_meta)
    try:
        if not _hot_path_enabled():
            raise RuntimeError("A98_HOT_PATH_DISABLED")
        from probability_hot_path import score_single
        result, hot_meta = score_single(
            market_df, payload.get("records") or [], market_key=str(payload.get("market_key") or ""),
            symbol=str(payload.get("symbol") or ""), sealed=bool(payload.get("sealed")),
            snapshot_key=str(payload.get("snapshot_key") or ""), progress=progress,
        )
        meta.update(dict(hot_meta or {})); meta["probability_hot_path"] = True
        return result
    except Exception as exc:
        if progress is not None:
            progress("HOT_PATH_FALLBACK", {"error_type": type(exc).__name__, "error_message": str(exc)})
        result = estimate_market_live_probability(
            market_df, payload.get("records") or [], market_key=str(payload.get("market_key") or ""),
            symbol=str(payload.get("symbol") or ""), sealed=bool(payload.get("sealed")),
        )
        meta.update(consume_probability_stage_timing() or _timing_from_result(result)); meta.update(_fallback_meta(exc))
        return result


def _execute_test_hook(mode: str, payload: Dict[str, Any], meta: Dict[str, Any], progress: Any) -> Tuple[Any, Dict[str, Any]]:
    if os.environ.get("AIN_PROBABILITY_WORKER_TEST_MODE") != "1":
        raise ValueError(f"UNKNOWN_PROBABILITY_WORKER_MODE:{mode}")
    if mode == "test_sleep":
        duration = max(0.0, float(payload.get("sleep_sec") or 0.0)); time.sleep(duration)
        return {"slept": duration}, meta
    if mode == "test_progress_sleep":
        duration = max(0.0, float(payload.get("sleep_sec") or 0.0)); deadline = time.monotonic() + duration; tick = 0
        while time.monotonic() < deadline:
            tick += 1
            if progress is not None: progress("TEST_PROGRESS", {"tick": tick})
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        return {"slept": duration, "progress_ticks": tick}, meta
    if mode == "test_raise":
        raise RuntimeError(str(payload.get("message") or "injected probability worker error"))
    raise ValueError(f"UNKNOWN_PROBABILITY_WORKER_MODE:{mode}")


def _execute(payload: Dict[str, Any], progress: Any = None) -> Tuple[Any, Dict[str, Any]]:
    """Execute one isolated request while keeping legacy dispatch compact."""
    mode = str(payload.get("mode") or "").strip().lower()
    meta = _base_execution_meta(payload)
    if mode == "prewarm":
        started = time.perf_counter()
        from probability_hot_path import full_pipeline_prewarm
        result = dict(full_pipeline_prewarm(progress=progress) or {})
        meta["model_load_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
        meta["pipeline_prewarmed"] = bool(result.get("pipeline_prewarmed"))
        return {**result, "pid": os.getpid(), "time": _utc_now()}, meta
    if mode == "ping":
        return {"ok": True, "pid": os.getpid(), "time": _utc_now()}, meta
    if mode == "batch":
        return _score_batch_request(payload, meta, progress), meta
    if mode == "single":
        return _score_single_request(payload, meta, progress), meta
    return _execute_test_hook(mode, payload, meta, progress)


def _watchdog_creation_flags() -> int:
    if os.name != "nt":
        return 0
    return int(getattr(subprocess, "CREATE_NO_WINDOW", 0)) | int(
        getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0)
    )


def _start_parent_watchdog(
    *,
    parent_pid: int,
    parent_start_time: float,
    worker_pid: int,
    worker_start_time: float,
) -> subprocess.Popen:
    return subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--watch-parent",
            "--parent-pid", str(int(parent_pid or 0)),
            "--parent-start-time", repr(float(parent_start_time or 0.0)),
            "--worker-pid", str(int(worker_pid or 0)),
            "--worker-start-time", repr(float(worker_start_time or 0.0)),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=_watchdog_creation_flags(),
        close_fds=(os.name != "nt"),
    )


def _stop_parent_watchdog(process: subprocess.Popen | None) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=1.0)
    except Exception:
        try:
            process.kill()
            process.wait(timeout=1.0)
        except Exception as exc:
            print(
                f"{_utc_now()} | WARNING | operation=stop_parent_watchdog | "
                f"type={type(exc).__name__} | message={exc}"
            )



def serve(
    work_dir: Path,
    log_dir: Path,
    *,
    parent_pid: int = 0,
    parent_start_time: float = 0.0,
) -> int:
    work_dir.mkdir(parents=True, exist_ok=True)
    log_path = _redirect_worker_log(log_dir)
    priority_warning = ""
    try:
        if os.name != "nt" and worker_low_priority_requested():
            os.nice(5)
    except Exception as exc:
        priority_warning = f"{type(exc).__name__}: {exc}"

    expected_parent_start = float(parent_start_time or 0.0) or _process_create_time(parent_pid)
    if not _process_is_same_and_alive(parent_pid, expected_parent_start):
        print(f"{_utc_now()} | PARENT_GONE_AT_START | parent_pid={int(parent_pid or 0)}")
        return 0

    worker_start_time = _process_create_time(os.getpid())
    watchdog: subprocess.Popen | None = None
    try:
        watchdog = _start_parent_watchdog(
            parent_pid=parent_pid, parent_start_time=expected_parent_start,
            worker_pid=os.getpid(), worker_start_time=worker_start_time,
        )
        parent_guard: Dict[str, Any] = {
            "mechanism": "EXTERNAL_PARENT_WATCHDOG", "watchdog_pid": int(watchdog.pid or 0),
            "parent_pid": int(parent_pid or 0), "parent_start_time": expected_parent_start,
            "worker_start_time": worker_start_time,
        }
    except Exception as exc:
        parent_guard = {
            "mechanism": "PARENT_PID_HEARTBEAT", "watchdog_pid": 0,
            "parent_pid": int(parent_pid or 0), "parent_start_time": expected_parent_start,
            "watchdog_error_type": type(exc).__name__, "watchdog_error_message": str(exc),
        }

    print(
        f"{_utc_now()} | START | pid={os.getpid()} | parent_pid={int(parent_pid or 0)} | "
        f"work_dir={work_dir} | log={log_path} | parent_guard={parent_guard}"
    )
    if priority_warning:
        print(f"{_utc_now()} | WARNING | operation=set_low_priority | error={priority_warning}")

    ready_path = work_dir / "ready.json"; heartbeat_path = work_dir / "heartbeat.json"; stop_path = work_dir / "STOP"
    _atomic_json(ready_path, {
        "pid": os.getpid(), "parent_pid": int(parent_pid or 0), "ready_at": _utc_now(),
        "protocol": 5, "parent_guard": parent_guard,
    })

    last_heartbeat = 0.0; handled = 0
    try:
        while not stop_path.exists():
            now = time.monotonic()
            if now - last_heartbeat >= HEARTBEAT_SEC:
                if not _process_is_same_and_alive(parent_pid, expected_parent_start):
                    print(f"{_utc_now()} | ORPHAN_EXIT | pid={os.getpid()} | parent_pid={int(parent_pid or 0)} | handled={handled}")
                    break
                try:
                    _atomic_json(heartbeat_path, {
                        "pid": os.getpid(), "parent_pid": int(parent_pid or 0), "time": _utc_now(),
                        "handled": handled, "parent_guard": parent_guard,
                    })
                except Exception as exc:
                    print(f"{_utc_now()} | WARNING | operation=write_heartbeat | type={type(exc).__name__} | message={exc}")
                last_heartbeat = now

            requests = sorted(work_dir.glob("request_*.pkl"), key=lambda path: path.name)
            if not requests:
                time.sleep(POLL_SEC); continue
            request_path = requests[0]; processing_path = request_path.with_suffix(".processing")
            try:
                replace_with_retry(request_path, processing_path, delays=(0.0, 0.01, 0.03, 0.08, 0.20, 0.50))
            except FileNotFoundError:
                continue

            request_id = request_path.stem.removeprefix("request_"); response_path = work_dir / f"response_{request_id}.pkl"
            started = time.perf_counter()
            try:
                payload = _load_request(processing_path)
                result, execution_meta = execute_with_progress(
                    heartbeat_path, request_id=request_id, payload=payload, handled=handled,
                    parent_guard=parent_guard, execute=_execute, write_json=_atomic_json, utc_now=_utc_now,
                )
                response = {
                    "ok": True, "request_id": request_id, "pid": os.getpid(), "completed_at": _utc_now(),
                    "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
                    "result": result, "execution_meta": execution_meta,
                }
                print(f"{_utc_now()} | OK | request_id={request_id} | mode={payload.get('mode')} | elapsed_ms={response['elapsed_ms']}")
            except BaseException as exc:
                response = {
                    "ok": False, "request_id": request_id, "pid": os.getpid(), "completed_at": _utc_now(),
                    "elapsed_ms": round((time.perf_counter() - started) * 1000.0, 3),
                    "error_type": type(exc).__name__, "error_message": str(exc),
                    "error_code": "PROBABILITY_SNAPSHOT_CACHE_MISS" if isinstance(exc, ProbabilitySnapshotCacheMiss) else "",
                    "traceback": traceback.format_exc(),
                }
                print(f"{_utc_now()} | ERROR | request_id={request_id} | type={type(exc).__name__} | message={exc}\n{response['traceback']}")
            try:
                _atomic_pickle(response_path, response)
            finally:
                handled += 1
                try:
                    processing_path.unlink(missing_ok=True)
                except Exception as exc:
                    print(f"{_utc_now()} | WARNING | operation=remove_processing_request | path={processing_path} | type={type(exc).__name__} | message={exc}")
    finally:
        print(f"{_utc_now()} | STOP | pid={os.getpid()} | handled={handled}")
        _stop_parent_watchdog(watchdog)
        try:
            ready_path.unlink(missing_ok=True)
        except Exception as exc:
            print(f"{_utc_now()} | WARNING | operation=remove_ready_marker | path={ready_path} | type={type(exc).__name__} | message={exc}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--serve", action="store_true")
    mode.add_argument("--watch-parent", action="store_true")
    parser.add_argument("--work-dir")
    parser.add_argument("--log-dir")
    parser.add_argument("--parent-pid", type=int, default=0)
    parser.add_argument("--parent-start-time", type=float, default=0.0)
    parser.add_argument("--worker-pid", type=int, default=0)
    parser.add_argument("--worker-start-time", type=float, default=0.0)
    args = parser.parse_args(argv)

    if args.watch_parent:
        return watch_parent(
            parent_pid=int(args.parent_pid or 0),
            parent_start_time=float(args.parent_start_time or 0.0),
            worker_pid=int(args.worker_pid or 0),
            worker_start_time=float(args.worker_start_time or 0.0),
        )
    if not args.work_dir or not args.log_dir:
        parser.error("--work-dir and --log-dir are required with --serve")
    return serve(
        Path(args.work_dir).resolve(),
        Path(args.log_dir).resolve(),
        parent_pid=int(args.parent_pid or 0),
        parent_start_time=float(args.parent_start_time or 0.0),
    )


if __name__ == "__main__":
    raise SystemExit(main())
