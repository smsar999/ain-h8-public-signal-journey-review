# -*- coding: utf-8 -*-
"""Crash-resilient coalescing outbox for trader-facing lifecycle rows.

Delivery identity is scoped to ``market + session_date + decision_lane + episode`` so radar
and model presentations of the same financial episode cannot overwrite one
another.  Primary and backup files are independently validated; one corrupt
copy is quarantined and repaired from the healthy sibling.  Both corrupt
copies fail closed and are never overwritten automatically.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import shutil
import threading
from pathlib import Path
from typing import Any, Iterable, Mapping

from atomic_io_utils import write_json_atomic
from json_truth_sanitizer import sanitize_json_value
from market_datetime_normalizer import session_date_text as _session_date_text

VERSION = "A110_SESSION_AWARE_UI_OUTBOX_ARCHIVE_V1"
_LOCK = threading.RLock()
_GENERATION_BY_PATH: dict[str, int] = {}


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _parse_time(value: Any) -> _dt.datetime:
    try:
        parsed = _dt.datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except Exception:
        return _dt.datetime.min.replace(tzinfo=_dt.timezone.utc)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=_dt.timezone.utc)


def outbox_path() -> Path:
    explicit = str(os.environ.get("AIN_LIVE_UI_OUTBOX_PATH", "") or "").strip()
    return Path(explicit) if explicit else Path(__file__).resolve().parent / "datainfo" / "live_ui_outbox" / "pending.json"


def backup_path(path: Path | None = None) -> Path:
    target = Path(path or outbox_path())
    return target.with_name(target.name + ".bak")


def archive_path(path: Path | None = None) -> Path:
    target = Path(path or outbox_path())
    return target.with_name(target.name + ".archive.jsonl")


def _lane(row: Mapping[str, Any]) -> str:
    return str(row.get("decision_lane") or row.get("delivery_lane") or "radar").strip().lower() or "radar"




def _row_session_date(row: Mapping[str, Any]) -> str:
    for key in (
        "session_date", "market_session_date", "trader_board_session_date",
        "live_sniper_session_date", "session_data_date", "data_session_date",
        "pulse_session_date", "latest_session_date",
    ):
        value = str((row or {}).get(key) or "").strip()
        if len(value) >= 10:
            return value[:10]
    for key in (
        "signal_bar_time", "bar_time", "first_cross_bar", "recommendation_datetime",
        "terminal_at", "session_close_at", "updated_at", "created_at",
    ):
        value = (row or {}).get(key)
        text = _session_date_text(value, market_key=str((row or {}).get("market_key") or ""))
        if text:
            return text
    return ""

def _identity(market: str, row: Mapping[str, Any]) -> str:
    episode = str(
        row.get("episode_key_sha256") or row.get("pulse_episode_id") or row.get("episode_id") or row.get("id")
        or f"{row.get('symbol')}|{row.get('signal_bar_time') or row.get('bar_time')}"
    )
    session = _row_session_date(row)
    return f"{market}|{session}|{_lane(row)}|{episode}"


def _delivery_id(logical: str) -> str:
    return hashlib.sha256(logical.encode("utf-8", "ignore")).hexdigest()


def _canonicalize_records(records: Mapping[str, Any]) -> tuple[dict[str, dict[str, Any]], bool]:
    """Migrate A102-A105 IDs without losing the newest retained revision."""
    normalized: dict[str, dict[str, Any]] = {}
    changed = False
    for old_key, raw in dict(records or {}).items():
        if not isinstance(raw, Mapping) or not isinstance(raw.get("row"), Mapping):
            changed = True
            continue
        record = dict(raw)
        row = dict(record.get("row") or {})
        market = str(record.get("market_key") or row.get("market_key") or "")
        logical = _identity(market, row)
        key = _delivery_id(logical)
        record.update({
            "delivery_id": key,
            "market_key": market,
            "decision_lane": _lane(row),
            "logical_identity": logical,
            "row": row,
        })
        if str(old_key) != key or str(raw.get("logical_identity") or "") != logical:
            changed = True
        previous = normalized.get(key)
        if previous is None:
            normalized[key] = record
            continue
        # A legacy file could contain duplicate representations.  Keep the
        # highest revision, then the newest timestamp; never merge two lanes.
        old_rank = (
            int(previous.get("revision") or 0),
            _parse_time(previous.get("updated_at") or previous.get("queued_at")),
        )
        new_rank = (
            int(record.get("revision") or 0),
            _parse_time(record.get("updated_at") or record.get("queued_at")),
        )
        if new_rank >= old_rank:
            normalized[key] = record
        changed = True
    return normalized, changed


def _decode(path: Path) -> tuple[dict[str, dict[str, Any]], int, _dt.datetime, bool]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("records", {}), dict):
        raise ValueError("INVALID_LIVE_UI_OUTBOX_PAYLOAD")
    records, migrated = _canonicalize_records(payload.get("records") or {})
    generation = max(0, int(payload.get("generation") or 0))
    updated = _parse_time(payload.get("updated_at"))
    return records, generation, updated, migrated


def _payload(records: Mapping[str, Mapping[str, Any]], generation: int) -> dict[str, Any]:
    return dict(sanitize_json_value({
        "version": VERSION,
        "generation": int(generation),
        "updated_at": _now(),
        "record_count": len(records),
        "records": dict(records),
    }) or {})


def _write_copy(path: Path, records: Mapping[str, Mapping[str, Any]], generation: int) -> None:
    write_json_atomic(
        path, _payload(records, generation), ensure_ascii=False, sort_keys=True,
        indent=2, trailing_newline=True, fsync_file=True, fsync_directory=True, allow_nan=False,
    )


def _quarantine_corrupt(path: Path) -> Path | None:
    if not path.exists():
        return None
    stamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    target = path.with_name(f"{path.name}.corrupt.{stamp}")
    try:
        path.replace(target)
    except OSError:
        shutil.copy2(path, target)
        path.unlink(missing_ok=True)
    return target


def _load_locked() -> dict[str, dict[str, Any]]:
    primary = outbox_path()
    backup = backup_path(primary)
    key = str(primary.resolve())
    if not primary.exists() and not backup.exists():
        _GENERATION_BY_PATH[key] = 0
        return {}

    decoded: dict[Path, tuple[dict[str, dict[str, Any]], int, _dt.datetime, bool]] = {}
    errors: dict[Path, BaseException] = {}
    for candidate in (primary, backup):
        if not candidate.exists():
            continue
        try:
            decoded[candidate] = _decode(candidate)
        except Exception as exc:
            errors[candidate] = exc

    if not decoded:
        detail = ";".join(f"{p.name}={type(e).__name__}:{e}" for p, e in errors.items())
        raise RuntimeError(f"LIVE_UI_OUTBOX_READ_FAILED:{primary}:{detail}")

    def rank(item: tuple[Path, tuple[dict[str, dict[str, Any]], int, _dt.datetime, bool]]):
        path, (_records, generation, updated, _migrated) = item
        try:
            mtime = path.stat().st_mtime_ns
        except OSError:
            mtime = 0
        return generation, updated, mtime

    chosen_path, (records, generation, _updated, migrated) = max(decoded.items(), key=rank)
    generation = max(0, int(generation))
    repair_needed = migrated or len(decoded) != 2
    if len(decoded) == 2:
        other_path = backup if chosen_path == primary else primary
        other_records, other_generation, _other_updated, other_migrated = decoded[other_path]
        repair_needed = repair_needed or other_migrated or other_generation != generation or other_records != records

    for corrupt_path in errors:
        _quarantine_corrupt(corrupt_path)
        repair_needed = True

    if repair_needed:
        generation += 1
        _write_copy(backup, records, generation)
        _write_copy(primary, records, generation)

    _GENERATION_BY_PATH[key] = generation
    return records


def _save_locked(records: Mapping[str, Mapping[str, Any]]) -> None:
    primary = outbox_path()
    backup = backup_path(primary)
    key = str(primary.resolve())
    generation = int(_GENERATION_BY_PATH.get(key, 0) or 0) + 1
    # Backup first: if the process dies before primary replace, startup selects
    # the copy with the greater generation and repairs its sibling.
    _write_copy(backup, records, generation)
    _write_copy(primary, records, generation)
    _GENERATION_BY_PATH[key] = generation


def enqueue(market_key: str, rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    market = str(market_key or "")
    frozen = [dict(row or {}) for row in (rows or []) if isinstance(row, Mapping)]
    if not frozen:
        return {"accepted": True, "durable": True, "queued": 0, "delivery_ids": []}
    ids: list[str] = []
    revisions: dict[str, int] = {}
    with _LOCK:
        records = _load_locked()
        for row in frozen:
            row_market = str(row.get("market_key") or market)
            logical = _identity(row_market, row)
            delivery_id = _delivery_id(logical)
            previous = dict(records.get(delivery_id) or {})
            revision = int(previous.get("revision") or 0) + 1
            records[delivery_id] = {
                "delivery_id": delivery_id,
                "revision": revision,
                "market_key": row_market,
                "session_date": _row_session_date(row),
                "decision_lane": _lane(row),
                "logical_identity": logical,
                "row": dict(sanitize_json_value(row) or {}),
                "queued_at": previous.get("queued_at") or _now(),
                "updated_at": _now(),
                "attempts": int(previous.get("attempts") or 0),
                "last_error": str(previous.get("last_error") or ""),
            }
            ids.append(delivery_id)
            revisions[delivery_id] = revision
        _save_locked(records)
    return {
        "accepted": True, "durable": True, "queued": len(ids),
        "delivery_ids": ids, "revisions": revisions, "path": str(outbox_path()),
        "backup_path": str(backup_path()),
    }


def pending(*, market_key: str = "", session_date: str | None = None) -> list[dict[str, Any]]:
    market = str(market_key or "")
    wanted_session = None if session_date is None else str(session_date or "")[:10]
    with _LOCK:
        records = _load_locked()
    rows = []
    for value in records.values():
        record = dict(value or {})
        if market and str(record.get("market_key") or "") != market:
            continue
        if wanted_session is not None and str(record.get("session_date") or _row_session_date(record.get("row") or {}))[:10] != wanted_session:
            continue
        rows.append(record)
    rows.sort(key=lambda item: (str(item.get("session_date") or ""), str(item.get("queued_at") or ""), str(item.get("decision_lane") or ""), str(item.get("delivery_id") or "")))
    return rows


def acknowledge(delivery_ids: Iterable[str], *, expected_revisions: Mapping[str, int] | None = None) -> int:
    wanted = {str(value or "") for value in delivery_ids if str(value or "")}
    if not wanted:
        return 0
    expected = {str(k): int(v or 0) for k, v in dict(expected_revisions or {}).items()}
    changed = 0
    with _LOCK:
        records = _load_locked()
        for key in wanted:
            current = records.get(key)
            if current is None:
                continue
            if key in expected and int((current or {}).get("revision") or 0) != int(expected[key]):
                continue
            records.pop(key, None)
            changed += 1
        if changed:
            _save_locked(records)
    return changed


def record_failure(delivery_ids: Iterable[str], error: Any) -> int:
    wanted = {str(value or "") for value in delivery_ids if str(value or "")}
    changed = 0
    with _LOCK:
        records = _load_locked()
        for key in wanted:
            if key not in records:
                continue
            row = dict(records[key] or {})
            row["attempts"] = int(row.get("attempts") or 0) + 1
            row["last_error"] = str(error or "UI_DELIVERY_FAILED")[:1000]
            row["last_attempt_at"] = _now()
            records[key] = row
            changed += 1
        if changed:
            _save_locked(records)
    return changed


def archive_before_session(*, market_key: str = "", session_date: str, max_items: int = 1000) -> int:
    """Move older live-delivery rows to an audit archive and ACK them from pending.

    The UI outbox is a live-delivery queue, not the source of financial truth.
    Rows from older sessions must not stay in the hot pending file forever and
    must never be emitted as today's live opportunities.  They are archived as
    JSONL before removal; if the archive write fails, pending is left untouched.
    """
    wanted_market = str(market_key or "")
    wanted_session = str(session_date or "")[:10]
    if not wanted_session:
        raise RuntimeError("LIVE_UI_ARCHIVE_REQUIRES_SESSION_DATE")
    limit = max(1, int(max_items or 1000))
    with _LOCK:
        records = _load_locked()
        selected: dict[str, dict[str, Any]] = {}
        for key, raw in records.items():
            record = dict(raw or {})
            if wanted_market and str(record.get("market_key") or "") != wanted_market:
                continue
            row_session = str(record.get("session_date") or _row_session_date(record.get("row") or {}))[:10]
            if row_session and row_session < wanted_session:
                selected[str(key)] = record
                if len(selected) >= limit:
                    break
        if not selected:
            return 0
        path = archive_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for record in selected.values():
                line = json.dumps(dict(sanitize_json_value(record) or {}), ensure_ascii=False, sort_keys=True)
                handle.write(line + "\n")
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError as exc:
                _ = exc
        for key in selected:
            records.pop(key, None)
        _save_locked(records)
        return len(selected)



def markets() -> list[str]:
    return sorted({str(item.get("market_key") or "") for item in pending() if item.get("market_key")})


def health() -> dict[str, Any]:
    path = outbox_path()
    try:
        rows = pending()
        return {"ok": True, "path": str(path), "backup_path": str(backup_path(path)), "archive_path": str(archive_path(path)), "pending": len(rows)}
    except Exception as exc:
        return {"ok": False, "path": str(path), "backup_path": str(backup_path(path)), "error": f"{type(exc).__name__}:{exc}"}


__all__ = [
    "VERSION", "outbox_path", "backup_path", "archive_path", "enqueue", "pending",
    "acknowledge", "record_failure", "archive_before_session", "markets", "health",
]
