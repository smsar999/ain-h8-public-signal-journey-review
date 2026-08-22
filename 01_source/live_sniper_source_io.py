# -*- coding: utf-8 -*-
"""Stable direct-source reads with one atomic MetaStock consumer contract."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from live_sniper_source_signature import source_signature_from_stat
from metastock_live_layout_guard import (
    MetaStockTailReadResult,
    SOURCE_CHANGED_DURING_READ,
    SOURCE_LAYOUT_COMPLETE,
    SOURCE_LAYOUT_INVALID,
    SOURCE_PARTIAL_WRITE,
    SOURCE_REWRITE_IN_PROGRESS,
    SOURCE_READ_ERROR,
)

VERSION = "A4_2_7_SOURCE_IO_WRITER_BUSY_V2"
SOURCE_READ_EXCEPTION = "SOURCE_READ_EXCEPTION"
SOURCE_RECORDS_EMPTY = "SOURCE_RECORDS_EMPTY"
SOURCE_FILE_CHANGED_DURING_READ = "SOURCE_FILE_CHANGED_DURING_READ"


@dataclass(frozen=True)
class StableSourceRead:
    records: Tuple[Mapping[str, Any], ...] = ()
    stat_before: Any = None
    stat_after: Any = None
    attempts: int = 0
    status_code: str = ""
    error_code: str = ""
    error_message: str = ""
    layout_offset: Optional[int] = None
    complete_record_count: int = 0
    partial_tail_bytes: int = 0
    last_complete_bar_time: str = ""
    last_complete_bar_hash: str = ""
    fresh_complete_record: bool = False
    retry_attempts: int = 0
    retry_delay_sec: float = 0.0
    retry_not_before_monotonic: float = 0.0
    generation_signature: Tuple[int, int] = (0, 0)

    @property
    def ok(self) -> bool:
        return bool(
            self.status_code == SOURCE_LAYOUT_COMPLETE
            and self.records and not self.error_code and self.stat_after is not None
        )

    @property
    def is_partial(self) -> bool:
        return self.status_code == SOURCE_PARTIAL_WRITE

    @property
    def is_rewrite_in_progress(self) -> bool:
        return self.status_code == SOURCE_REWRITE_IN_PROGRESS

    @property
    def is_writer_busy(self) -> bool:
        return self.status_code in {SOURCE_PARTIAL_WRITE, SOURCE_REWRITE_IN_PROGRESS}


def _signature(stat_result: Any) -> Tuple[int, int]:
    """Canonical source signature: ``(mtime_ns, size_bytes)``."""
    signature = source_signature_from_stat(stat_result)
    return (signature.mtime_ns, signature.size)


def _legacy_result(
    path: Path, fields: int, records: Sequence[Mapping[str, Any]], stat_after: Any,
) -> MetaStockTailReadResult:
    return MetaStockTailReadResult.classified(
        path, int(fields), SOURCE_LAYOUT_COMPLETE if records else SOURCE_RECORDS_EMPTY,
        stat_result=stat_after, records=records,
    )


def _stable_from_result(
    result: MetaStockTailReadResult, *, stat_before: Any, stat_after: Any, attempt: int,
    error_code: str = "", error_message: str = "",
) -> StableSourceRead:
    return StableSourceRead(
        records=tuple(dict(item or {}) for item in result.records),
        stat_before=stat_before, stat_after=stat_after, attempts=int(attempt),
        status_code=str(result.code or ""), error_code=str(error_code or ""),
        error_message=str(error_message or ""), layout_offset=result.layout_offset,
        complete_record_count=int(result.complete_record_count or 0),
        partial_tail_bytes=int(result.partial_tail_bytes or 0),
        last_complete_bar_time=str(result.last_complete_bar_time or ""),
        last_complete_bar_hash=str(result.last_complete_bar_hash or ""),
        fresh_complete_record=bool(result.fresh_complete_record),
        retry_attempts=int(result.retry_attempts or 0),
        retry_delay_sec=float(result.retry_delay_sec or 0.0),
        retry_not_before_monotonic=float(result.retry_not_before_monotonic or 0.0),
        generation_signature=tuple(result.signature or (0, 0)),
    )


def read_stable_source_records(
    path: Any, *, reader: Callable[..., Any], fields: int,
    attempts: int = 2, max_valid: int = 72, scan_depth: int = 160,
) -> StableSourceRead:
    """Read records and status atomically; never query a side registry afterward."""
    target = Path(path)
    last_before = last_after = None
    for attempt in range(1, max(1, int(attempts)) + 1):
        try:
            last_before = os.stat(target)
            try:
                raw_result = reader(
                    str(target), int(fields), max_valid=int(max_valid), scan_depth=int(scan_depth),
                    use_persistent_cache=False, return_result=True,
                )
            except TypeError as exc:
                # Compatibility for isolated tests/adapters that still expose the
                # historical list-only signature.  Production readers use the
                # structured branch above.
                if "return_result" not in str(exc):
                    raise
                legacy_records = reader(
                    str(target), int(fields), max_valid=int(max_valid), scan_depth=int(scan_depth),
                    use_persistent_cache=False,
                )
                raw_result = _legacy_result(target, int(fields), legacy_records, os.stat(target))
            last_after = os.stat(target)
        except Exception as exc:
            return StableSourceRead(
                stat_before=last_before, stat_after=last_after, attempts=attempt,
                status_code=SOURCE_READ_ERROR, error_code=SOURCE_READ_EXCEPTION,
                error_message=f"{type(exc).__name__}: {exc}",
            )

        if isinstance(raw_result, MetaStockTailReadResult):
            result = raw_result
        else:
            result = _legacy_result(target, int(fields), list(raw_result or ()), last_after)

        before_sig = _signature(last_before)
        after_sig = _signature(last_after)
        result_sig = tuple(result.signature or (0, 0))
        coherent_signature = before_sig == after_sig and result_sig == after_sig

        if result.is_writer_busy:
            if coherent_signature:
                code = str(result.code or SOURCE_PARTIAL_WRITE)
                message = (
                    "TickerChart is rewriting the current MetaStock bar; the previous bar is retained internally and not republished."
                    if code == SOURCE_REWRITE_IN_PROGRESS else
                    "TickerChart is writing an incomplete MetaStock record; "
                    f"offset={result.layout_offset} partial_bytes={result.partial_tail_bytes} "
                    f"complete_records={result.complete_record_count}"
                )
                return _stable_from_result(
                    result, stat_before=last_before, stat_after=last_after, attempt=attempt,
                    error_code=code, error_message=message,
                )
            continue

        if result.code == SOURCE_CHANGED_DURING_READ or not coherent_signature:
            continue

        if result.ok:
            return _stable_from_result(
                result, stat_before=last_before, stat_after=last_after, attempt=attempt,
            )

        if result.code in {SOURCE_LAYOUT_INVALID, SOURCE_READ_ERROR}:
            return _stable_from_result(
                result, stat_before=last_before, stat_after=last_after, attempt=attempt,
                error_code=SOURCE_READ_EXCEPTION,
                error_message=str(result.detail or result.code),
            )

        return _stable_from_result(
            result, stat_before=last_before, stat_after=last_after, attempt=attempt,
            error_code=SOURCE_RECORDS_EMPTY,
            error_message="The source reader returned no valid records.",
        )

    # A live MetaStock writer can legitimately replace/truncate the current DAT
    # generation while we are reading it.  Exhausting the tiny coherence retry
    # window proves writer ownership, not source corruption.  Preserve the last
    # good in-memory bar and defer this generation without error/failure budget.
    final_signature = _signature(last_after) if last_after is not None else (0, 0)
    return StableSourceRead(
        stat_before=last_before, stat_after=last_after, attempts=max(1, int(attempts)),
        status_code=SOURCE_REWRITE_IN_PROGRESS,
        error_code=SOURCE_REWRITE_IN_PROGRESS,
        error_message="TickerChart changed the source generation during every bounded read attempt; retry deferred.",
        retry_attempts=max(1, int(attempts)), retry_delay_sec=0.20,
        generation_signature=final_signature,
    )


def source_read_error_observation(
    result: StableSourceRead, *, market_key: str, symbol: str, path: str,
    log_event: Callable[..., Any],
) -> Optional[dict[str, Any]]:
    if result.ok:
        return None
    if result.is_writer_busy:
        # Normal writer-owned state: no error log, no observation, no failure budget.
        code = str(result.status_code or result.error_code or SOURCE_PARTIAL_WRITE)
        return {
            "_source_read_partial": bool(code == SOURCE_PARTIAL_WRITE),
            "_source_read_writer_busy": True,
            "_source_read_error": False,
            "_source_read_status": code,
            "source_read_error_code": code,
            "source_read_error_message": str(result.error_message or code),
            "source_read_attempts": int(result.attempts or 0),
            "source_layout_offset": result.layout_offset,
            "source_complete_record_count": int(result.complete_record_count or 0),
            "source_partial_tail_bytes": int(result.partial_tail_bytes or 0),
            "source_retry_attempts": int(result.retry_attempts or 0),
            "source_retry_delay_sec": float(result.retry_delay_sec or 0.0),
            "source_retry_not_before_monotonic": float(result.retry_not_before_monotonic or 0.0),
            "source_generation_signature": tuple(result.generation_signature or (0, 0)),
            "source_last_complete_bar_time": str(result.last_complete_bar_time or ""),
            "source_last_complete_bar_hash": str(result.last_complete_bar_hash or ""),
            "market_key": str(market_key), "symbol": str(symbol), "source_file": str(path),
        }
    code = str(result.error_code or SOURCE_READ_EXCEPTION)
    message = str(result.error_message or code)
    log_event(
        code, "تعذر إنتاج لقطة مصدر متماسكة لمسار القنص المباشر",
        market=str(market_key), symbol=str(symbol), source_file=str(path),
        read_attempts=int(result.attempts or 0), error_message=message,
    )
    return {
        "_source_read_error": True, "source_read_error_code": code,
        "source_read_error_message": message, "source_read_attempts": int(result.attempts or 0),
        "source_read_status": str(result.status_code or ""),
        "market_key": str(market_key), "symbol": str(symbol), "source_file": str(path),
    }


def stable_read_parts(result: StableSourceRead) -> tuple[list[dict[str, Any]], Any, Any, int]:
    return (
        [dict(item or {}) for item in result.records], result.stat_before,
        result.stat_after, int(result.attempts or 0),
    )


__all__ = [
    "VERSION", "StableSourceRead", "SOURCE_READ_EXCEPTION", "SOURCE_RECORDS_EMPTY",
    "SOURCE_FILE_CHANGED_DURING_READ", "SOURCE_PARTIAL_WRITE", "SOURCE_REWRITE_IN_PROGRESS",
    "read_stable_source_records",
    "source_read_error_observation", "stable_read_parts",
]
