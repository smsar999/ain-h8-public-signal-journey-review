# -*- coding: utf-8 -*-
"""Durable single authority for terminal Episode truth and projection debt.

Terminal state is accepted only after a SQLite/WAL ``FULL`` commit.  The same
transaction creates one durable projection debt so a process death after the
Terminal COMMIT but before journal/UI publication is replayable after restart.
JSON candidate/veto files remain diagnostic mirrors; they cannot author or erase
terminal truth.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import re
import sqlite3
import threading
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional

from atomic_io_utils import (
    DURABLE_STATE_REPLACE_DELAYS,
    sync_parent_directory,
    write_json_atomic,
)

VERSION = "A4_2_14_TERMINAL_TRUTH_AUTHORITY_V3"
PROJECTION_CONTRACT = "A4_2_14_TERMINAL_PROJECTION_OUTBOX_V1"
_LOCKS: Dict[str, threading.RLock] = {}
_LOCKS_GUARD = threading.RLock()


class TerminalTruthError(RuntimeError):
    pass


class TerminalTruthConflict(TerminalTruthError):
    pass


class TerminalTruthPersistenceError(TerminalTruthError):
    pass


def _utc_now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="microseconds")


def _canonical_json(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value or {}), ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def terminal_truth_db_path(root_hint: Optional[Path] = None) -> Path:
    explicit = str(os.environ.get("AIN_TERMINAL_TRUTH_DB_PATH") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve(strict=False)
    root = str(os.environ.get("AIN_PULSE_TICK_TAPE_DIR") or "").strip()
    if not root:
        runtime = str(os.environ.get("AIN_RUNTIME_SESSION_DIR") or "").strip()
        if runtime:
            root = str(Path(runtime) / "pulse_tick_tape")
        elif root_hint is not None:
            root = str(Path(root_hint))
        else:
            root = str(Path(tempfile.gettempdir()) / f"ain-terminal-truth-{os.getpid()}")
    return (Path(root).expanduser().resolve(strict=False) / "terminal_truth.sqlite")


def _path_lock(path: Path) -> threading.RLock:
    key = str(path)
    with _LOCKS_GUARD:
        return _LOCKS.setdefault(key, threading.RLock())


def _episode_aliases(value: str) -> tuple[str, ...]:
    raw = str(value or "").strip()
    if not raw:
        return ()
    aliases = [raw]
    if re.search(r"T\d{6}\.000(?:-|$)", raw):
        aliases.append(re.sub(r"(T\d{6})\.000(?=-|$)", r"\1", raw))
    elif re.search(r"T\d{6}(?:-|$)", raw):
        aliases.append(re.sub(r"(T\d{6})(?=-|$)", r"\1.000", raw))
    return tuple(dict.fromkeys(aliases))


def _expanded_aliases(episode_id: str, aliases: Iterable[str] = ()) -> tuple[str, ...]:
    values: list[str] = []
    for item in (episode_id, *tuple(aliases or ())):
        for alias in _episode_aliases(str(item or "")):
            if alias and alias not in values:
                values.append(alias)
    return tuple(values)


def _preferred_canonical_id(aliases: Iterable[str]) -> str:
    values = tuple(str(x or "").strip() for x in aliases if str(x or "").strip())
    for value in values:
        if re.search(r"T\d{6}\.000(?:-|$)", value):
            return value
    return values[0] if values else ""


def _semantic_terminal_payload(
    *, episode_id: str, terminal_state: str, market_key: str, symbol: str, row: Mapping[str, Any],
) -> Dict[str, Any]:
    source = dict(row or {})

    def _first(*keys: str) -> Any:
        for key in keys:
            value = source.get(key)
            if value not in (None, ""):
                return value
        return None

    return {
        "episode_id": str(episode_id or ""),
        "terminal_state": str(terminal_state or "").upper(),
        "market_key": str(market_key or source.get("market_key") or ""),
        "symbol": str(symbol or source.get("symbol") or "").strip().upper(),
        "signal_bar_time": str(_first("signal_bar_time", "bar_time", "first_cross_bar") or ""),
        "source_observation_id": str(_first(
            "final_source_observation_id", "sealed_close_source_observation_id", "source_observation_id",
        ) or ""),
        "sealed_truth_id": str(_first("sealed_truth_id", "effective_truth_id") or ""),
        "reason_code": str(_first(
            "seal_failure_reason_code", "terminal_reason_code", "reason_code",
        ) or "").upper(),
        "p50_final": _first("p50_final", "p50_sealed"),
        "p100_final": _first("p100_final", "p100_sealed"),
        "anchor": _first("sealed_gann_anchor_price", "sealed_model_anchor_price"),
        "r1": _first("sealed_gann_r1_price", "sealed_r1_price"),
        "r50": _first("sealed_gann_r50_price", "sealed_r50_price"),
        "r100": _first("sealed_gann_r100_price", "sealed_r100_price"),
        "stop": _first("sealed_gann_stop_price", "sealed_stop_price"),
    }


def _semantic_hash(payload: Mapping[str, Any]) -> str:
    blob = _canonical_json(payload)
    return hashlib.sha256(("TERMINAL_SEMANTIC_V1\0" + blob).encode("utf-8")).hexdigest()


def _connect(path: Optional[Path] = None) -> sqlite3.Connection:
    target = Path(path or terminal_truth_db_path())
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), timeout=10.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS terminal_truth (
            episode_id TEXT PRIMARY KEY,
            terminal_state TEXT NOT NULL,
            market_key TEXT NOT NULL DEFAULT '',
            symbol TEXT NOT NULL DEFAULT '',
            terminal_at TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            semantic_hash TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS terminal_truth_alias (
            alias TEXT PRIMARY KEY,
            episode_id TEXT NOT NULL,
            FOREIGN KEY(episode_id) REFERENCES terminal_truth(episode_id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS terminal_projection_outbox (
            episode_id TEXT PRIMARY KEY,
            terminal_state TEXT NOT NULL,
            market_key TEXT NOT NULL DEFAULT '',
            symbol TEXT NOT NULL DEFAULT '',
            terminal_at TEXT NOT NULL,
            reason TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL,
            payload_hash TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'PENDING',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            acknowledged_at TEXT NOT NULL DEFAULT '',
            FOREIGN KEY(episode_id) REFERENCES terminal_truth(episode_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_terminal_truth_market_symbol
            ON terminal_truth(market_key, symbol, terminal_at);
        CREATE INDEX IF NOT EXISTS idx_terminal_projection_status
            ON terminal_projection_outbox(status, terminal_at, episode_id);
        """
    )
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(terminal_truth)")}
    if "semantic_hash" not in columns:
        conn.execute("ALTER TABLE terminal_truth ADD COLUMN semantic_hash TEXT NOT NULL DEFAULT ''")
    return conn


def _row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    try:
        payload = dict(json.loads(str(row["payload_json"] or "{}")))
    except Exception:
        payload = {}
    payload.update({
        "episode_id": str(row["episode_id"] or ""),
        "terminal_state": str(row["terminal_state"] or ""),
        "market_key": str(row["market_key"] or ""),
        "symbol": str(row["symbol"] or ""),
        "terminal_at": str(row["terminal_at"] or ""),
        "reason": str(row["reason"] or ""),
        "payload_hash": str(row["payload_hash"] or ""),
        "semantic_hash": str(row["semantic_hash"] or "") if "semantic_hash" in row.keys() else "",
        "terminal_truth_version": VERSION,
        "terminal_truth_durable": True,
    })
    return payload


def _projection_row_to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    try:
        payload = dict(json.loads(str(row["payload_json"] or "{}")))
    except Exception:
        payload = {}
    payload.update({
        "episode_id": str(row["episode_id"] or ""),
        "terminal_state": str(row["terminal_state"] or ""),
        "market_key": str(row["market_key"] or ""),
        "symbol": str(row["symbol"] or ""),
        "terminal_at": str(row["terminal_at"] or ""),
        "reason": str(row["reason"] or ""),
        "payload_hash": str(row["payload_hash"] or ""),
        "projection_status": str(row["status"] or "PENDING").upper(),
        "projection_attempts": int(row["attempts"] or 0),
        "projection_last_error": str(row["last_error"] or ""),
        "projection_acknowledged_at": str(row["acknowledged_at"] or ""),
        "terminal_projection_contract": PROJECTION_CONTRACT,
        "terminal_truth_durable": True,
        "terminal_projection_pending": str(row["status"] or "").upper() != "ACKED",
    })
    return payload


def _find_existing(conn: sqlite3.Connection, aliases: Iterable[str]) -> Optional[sqlite3.Row]:
    for key in aliases:
        row = conn.execute("SELECT * FROM terminal_truth WHERE episode_id=?", (key,)).fetchone()
        if row is None:
            row = conn.execute(
                "SELECT t.* FROM terminal_truth_alias a JOIN terminal_truth t ON t.episode_id=a.episode_id WHERE a.alias=?",
                (key,),
            ).fetchone()
        if row is not None:
            return row
    return None


def _ensure_projection_debt(
    conn: sqlite3.Connection, *, episode_id: str, terminal_state: str, market_key: str,
    symbol: str, terminal_at: str, reason: str, payload_json: str, payload_hash: str,
) -> None:
    now = _utc_now()
    conn.execute(
        """INSERT OR IGNORE INTO terminal_projection_outbox(
               episode_id,terminal_state,market_key,symbol,terminal_at,reason,
               payload_json,payload_hash,status,attempts,last_error,created_at,updated_at,acknowledged_at
           ) VALUES(?,?,?,?,?,?,?,?, 'PENDING',0,'',?,?, '')""",
        (episode_id, terminal_state, market_key, symbol, terminal_at, reason, payload_json, payload_hash, now, now),
    )
    # Existing unacknowledged debt receives the canonical payload.  ACKED rows are
    # immutable receipts and are deliberately not reopened by an idempotent repeat.
    conn.execute(
        """UPDATE terminal_projection_outbox
              SET terminal_state=?,market_key=?,symbol=?,terminal_at=?,reason=?,
                  payload_json=?,payload_hash=?,updated_at=?
            WHERE episode_id=? AND status!='ACKED'""",
        (terminal_state, market_key, symbol, terminal_at, reason, payload_json, payload_hash, now, episode_id),
    )



def _intent_dir(path: Optional[Path] = None) -> Path:
    target = Path(path or terminal_truth_db_path())
    return target.parent / (target.name + ".terminal_intents")


def _intent_path(episode_id: str, terminal_state: str, *, path: Optional[Path] = None) -> Path:
    seed = f"{str(episode_id or '').strip()}\0{str(terminal_state or '').strip().upper()}"
    name = hashlib.sha256(seed.encode("utf-8")).hexdigest() + ".json"
    return _intent_dir(path) / name


def prepare_terminal_transition_intent(
    episode_id: str, terminal_state: str, *, aliases: Iterable[str] = (), market_key: str = "",
    symbol: str = "", at: str = "", reason: str = "", row: Optional[Mapping[str, Any]] = None,
    path: Optional[Path] = None,
) -> Dict[str, Any]:
    """Durably stage one exact terminal proposal before SQLite authority.

    Only one semantic proposal may be pending for an Episode alias family.  A
    retry of the same proposal reuses the first fsynced intent byte-for-byte;
    a competing terminal state or payload fails closed before either proposal
    can be applied in memory.  This scan is terminal-only and never touches the
    ordinary price/update path.
    """
    aliases_all = _expanded_aliases(str(episode_id or ""), aliases)
    canonical_id = _preferred_canonical_id(aliases_all)
    state = str(terminal_state or "").strip().upper()
    if not canonical_id or not state:
        raise TerminalTruthPersistenceError("TERMINAL_INTENT_EPISODE_OR_STATE_MISSING")
    target_db = Path(path or terminal_truth_db_path())
    target = _intent_path(canonical_id, state, path=target_db)
    authority_row = dict(row or {})
    market = str(market_key or authority_row.get("market_key") or "")
    sym = str(symbol or authority_row.get("symbol") or "").strip().upper()
    semantic_payload = _semantic_terminal_payload(
        episode_id=canonical_id, terminal_state=state,
        market_key=market, symbol=sym, row=authority_row,
    )
    semantic_hash = _semantic_hash(semantic_payload)
    lock = _path_lock(target_db)
    with lock:
        target.parent.mkdir(parents=True, exist_ok=True)
        requested_aliases = set(aliases_all)
        for item in sorted(target.parent.glob("*.json")):
            try:
                existing = dict(json.loads(item.read_text(encoding="utf-8")))
            except Exception as exc:
                raise TerminalTruthPersistenceError(
                    f"TERMINAL_INTENT_CORRUPT:{item}:{type(exc).__name__}:{exc}"
                ) from exc
            existing_aliases = set(_expanded_aliases(
                str(existing.get("episode_id") or ""), tuple(existing.get("aliases") or ()),
            ))
            if not requested_aliases.intersection(existing_aliases):
                continue
            existing_id = _preferred_canonical_id(existing_aliases) or canonical_id
            existing_state = str(existing.get("terminal_state") or "").strip().upper()
            existing_row = dict(existing.get("row") or {})
            existing_semantic = _semantic_terminal_payload(
                episode_id=existing_id, terminal_state=existing_state,
                market_key=str(existing.get("market_key") or existing_row.get("market_key") or ""),
                symbol=str(existing.get("symbol") or existing_row.get("symbol") or "").strip().upper(),
                row=existing_row,
            )
            existing_hash = str(existing.get("semantic_hash") or _semantic_hash(existing_semantic))
            if existing_state != state or existing_hash != semantic_hash:
                raise TerminalTruthConflict(
                    "TERMINAL_INTENT_CONFLICT:"
                    f"episode={canonical_id}:existing_state={existing_state}:requested_state={state}:"
                    f"existing_hash={existing_hash}:requested_hash={semantic_hash}"
                )
            return {
                "prepared": True, "idempotent": True, "path": str(item),
                **existing, "semantic_hash": existing_hash,
            }

        payload = {
            "contract": "A4_2_14_TERMINAL_TRANSITION_INTENT_V2",
            "episode_id": canonical_id,
            "aliases": list(aliases_all),
            "terminal_state": state,
            "market_key": market,
            "symbol": sym,
            "at": str(at or _utc_now()),
            "reason": str(reason or authority_row.get("reason") or state),
            "row": authority_row,
            "semantic_hash": semantic_hash,
            "prepared_at": _utc_now(),
        }
        try:
            write_json_atomic(
                target, payload, ensure_ascii=False, sort_keys=True, indent=None,
                trailing_newline=True, allow_nan=True, default=str,
                fsync_file=True, fsync_directory=True,
                delays=DURABLE_STATE_REPLACE_DELAYS,
            )
        except Exception as exc:
            raise TerminalTruthPersistenceError(
                f"TERMINAL_INTENT_PREPARE_FAILED:{type(exc).__name__}:{exc}"
            ) from exc
        return {"prepared": True, "idempotent": False, "path": str(target), **payload}


def clear_terminal_transition_intent(
    episode_id: str, terminal_state: str, *, path: Optional[Path] = None,
) -> bool:
    target = _intent_path(str(episode_id or ""), str(terminal_state or ""), path=path)
    try:
        existed = target.exists()
        target.unlink(missing_ok=True)
        if existed:
            # POSIX persists the deletion via parent-directory fsync.  Windows
            # may replay a stale intent after sudden power loss; replay is
            # idempotent and therefore safer than treating deletion as authority.
            sync_parent_directory(target)
        return True
    except Exception:
        return False


def pending_terminal_transition_intents(*, path: Optional[Path] = None) -> list[Dict[str, Any]]:
    root = _intent_dir(path)
    if not root.exists():
        return []
    rows: list[Dict[str, Any]] = []
    for item in sorted(root.glob("*.json")):
        try:
            payload = dict(json.loads(item.read_text(encoding="utf-8")))
            payload["intent_path"] = str(item)
            rows.append(payload)
        except Exception as exc:
            raise TerminalTruthPersistenceError(
                f"TERMINAL_INTENT_CORRUPT:{item}:{type(exc).__name__}:{exc}"
            ) from exc
    return rows


def reconcile_terminal_transition_intents(*, path: Optional[Path] = None) -> Dict[str, Any]:
    target = Path(path or terminal_truth_db_path())
    committed = failed = 0
    errors: list[str] = []
    for intent in pending_terminal_transition_intents(path=target):
        try:
            receipt = commit_terminal_truth(
                str(intent.get("episode_id") or ""),
                str(intent.get("terminal_state") or ""),
                aliases=tuple(intent.get("aliases") or ()),
                market_key=str(intent.get("market_key") or ""),
                symbol=str(intent.get("symbol") or ""),
                at=str(intent.get("at") or ""),
                reason=str(intent.get("reason") or ""),
                row=dict(intent.get("row") or {}),
                path=target,
            )
            if bool(receipt.get("terminal_truth_durable")):
                clear_terminal_transition_intent(
                    str(intent.get("episode_id") or ""),
                    str(intent.get("terminal_state") or ""),
                    path=target,
                )
                committed += 1
            else:
                failed += 1
        except Exception as exc:
            failed += 1
            errors.append(f"{type(exc).__name__}:{exc}")
    return {"attempted": committed + failed, "committed": committed, "failed": failed, "errors": errors}


def commit_terminal_truth(
    episode_id: str,
    terminal_state: str,
    *,
    aliases: Iterable[str] = (),
    market_key: str = "",
    symbol: str = "",
    at: str = "",
    reason: str = "",
    row: Optional[Mapping[str, Any]] = None,
    path: Optional[Path] = None,
) -> Dict[str, Any]:
    requested_id = str(episode_id or "").strip()
    state = str(terminal_state or "").strip().upper()
    if not requested_id or not state:
        raise TerminalTruthPersistenceError("TERMINAL_TRUTH_EPISODE_OR_STATE_MISSING")
    canonical_aliases = _expanded_aliases(requested_id, aliases)
    target = Path(path or terminal_truth_db_path())
    lock = _path_lock(target)
    with lock:
        conn = _connect(target)
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = _find_existing(conn, canonical_aliases)
            canonical_id = str(existing["episode_id"] or "") if existing is not None else _preferred_canonical_id(canonical_aliases)
            if not canonical_id:
                raise TerminalTruthPersistenceError("TERMINAL_TRUTH_CANONICAL_EPISODE_MISSING")
            canonical_aliases = tuple(dict.fromkeys((canonical_id, *_expanded_aliases(canonical_id, canonical_aliases))))
            terminal_at = str(at or _utc_now())
            payload = dict(row or {})
            payload.update({
                "episode_id": canonical_id,
                "terminal_state": state,
                "market_key": str(market_key or payload.get("market_key") or ""),
                "symbol": str(symbol or payload.get("symbol") or "").strip().upper(),
                "terminal_at": terminal_at,
                "reason": str(reason or payload.get("reason") or state),
                "episode_aliases": list(canonical_aliases),
                "terminal_projection_contract": PROJECTION_CONTRACT,
            })
            semantic_payload = _semantic_terminal_payload(
                episode_id=canonical_id, terminal_state=state,
                market_key=str(payload.get("market_key") or ""),
                symbol=str(payload.get("symbol") or ""), row=payload,
            )
            semantic_hash = _semantic_hash(semantic_payload)
            payload["terminal_semantic_hash"] = semantic_hash
            payload_json = _canonical_json(payload)
            payload_hash = _sha256_text(payload_json)

            if existing is not None:
                existing_state = str(existing["terminal_state"] or "").upper()
                existing_semantic_hash = str(existing["semantic_hash"] or "") if "semantic_hash" in existing.keys() else ""
                if not existing_semantic_hash:
                    existing_semantic_hash = _semantic_hash(_semantic_terminal_payload(
                        episode_id=canonical_id,
                        terminal_state=existing_state,
                        market_key=str(existing["market_key"] or ""),
                        symbol=str(existing["symbol"] or ""),
                        row=_row_to_dict(existing),
                    ))
                if existing_state != state or existing_semantic_hash != semantic_hash:
                    raise TerminalTruthConflict(
                        f"TERMINAL_TRUTH_CONFLICT:{requested_id}:{existing_state}:{existing_semantic_hash}!={state}:{semantic_hash}"
                    )
                for alias in canonical_aliases:
                    mapped = conn.execute("SELECT episode_id FROM terminal_truth_alias WHERE alias=?", (alias,)).fetchone()
                    if mapped is not None and str(mapped[0]) != canonical_id:
                        raise TerminalTruthConflict(
                            f"TERMINAL_TRUTH_ALIAS_CONFLICT:{alias}:{mapped[0]}!={canonical_id}"
                        )
                    conn.execute("INSERT OR IGNORE INTO terminal_truth_alias(alias,episode_id) VALUES(?,?)", (alias, canonical_id))
                existing_payload = _row_to_dict(existing)
                projection_payload = dict(existing_payload)
                projection_payload["episode_aliases"] = list(canonical_aliases)
                projection_json = _canonical_json(projection_payload)
                projection_hash = _sha256_text(projection_json)
                _ensure_projection_debt(
                    conn, episode_id=canonical_id, terminal_state=existing_state,
                    market_key=str(existing["market_key"] or ""), symbol=str(existing["symbol"] or ""),
                    terminal_at=str(existing["terminal_at"] or terminal_at), reason=str(existing["reason"] or state),
                    payload_json=projection_json, payload_hash=projection_hash,
                )
                conn.execute("COMMIT")
                out = existing_payload
                out.update({
                    "episode_aliases": list(canonical_aliases),
                    "recorded": True, "durable": True, "created": False, "idempotent": True,
                    "semantic_hash": existing_semantic_hash,
                    "terminal_projection_pending": bool(
                        terminal_projection_for_episode(canonical_id, path=target).get("terminal_projection_pending", True)
                    ),
                })
                return out

            conn.execute(
                """INSERT INTO terminal_truth(
                       episode_id,terminal_state,market_key,symbol,terminal_at,reason,
                       payload_json,payload_hash,semantic_hash,created_at
                   ) VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    canonical_id, state, str(payload.get("market_key") or ""),
                    str(payload.get("symbol") or ""), terminal_at,
                    str(payload.get("reason") or state), payload_json, payload_hash, semantic_hash, _utc_now(),
                ),
            )
            for alias in canonical_aliases:
                conn.execute("INSERT INTO terminal_truth_alias(alias,episode_id) VALUES(?,?)", (alias, canonical_id))
            _ensure_projection_debt(
                conn, episode_id=canonical_id, terminal_state=state,
                market_key=str(payload.get("market_key") or ""), symbol=str(payload.get("symbol") or ""),
                terminal_at=terminal_at, reason=str(payload.get("reason") or state),
                payload_json=payload_json, payload_hash=payload_hash,
            )
            conn.execute("COMMIT")
            payload.update({
                "payload_hash": payload_hash,
                "semantic_hash": semantic_hash,
                "terminal_truth_version": VERSION,
                "terminal_truth_durable": True,
                "terminal_projection_pending": True,
                "recorded": True,
                "durable": True,
                "created": True,
                "idempotent": False,
            })
            return payload
        except Exception as exc:
            rollback_error = ""
            try:
                conn.execute("ROLLBACK")
            except Exception as rollback_exc:
                rollback_error = f"{type(rollback_exc).__name__}:{rollback_exc}"
            if rollback_error:
                try:
                    setattr(exc, "rollback_error", rollback_error)
                except Exception as attach_exc:
                    rollback_error = (
                        rollback_error
                        + f";ROLLBACK_ERROR_ATTACHMENT_FAILED:{type(attach_exc).__name__}:{attach_exc}"
                    )
            if isinstance(exc, TerminalTruthError):
                raise
            raise TerminalTruthPersistenceError(f"TERMINAL_TRUTH_COMMIT_FAILED:{type(exc).__name__}:{exc}") from exc
        finally:
            conn.close()


def terminal_truth_for_episode(
    episode_id: str, *, aliases: Iterable[str] = (), path: Optional[Path] = None,
) -> Dict[str, Any]:
    eid = str(episode_id or "").strip()
    if not eid:
        return {}
    target = Path(path or terminal_truth_db_path())
    if not target.exists():
        return {}
    keys = _expanded_aliases(eid, aliases)
    lock = _path_lock(target)
    with lock:
        conn = _connect(target)
        try:
            row = _find_existing(conn, keys)
            return _row_to_dict(row) if row is not None else {}
        finally:
            conn.close()


def load_terminal_truths(*, path: Optional[Path] = None) -> list[Dict[str, Any]]:
    target = Path(path or terminal_truth_db_path())
    if not target.exists():
        return []
    lock = _path_lock(target)
    with lock:
        conn = _connect(target)
        try:
            rows = conn.execute("SELECT * FROM terminal_truth ORDER BY terminal_at,episode_id").fetchall()
            aliases_by_id: Dict[str, list[str]] = {}
            for alias_row in conn.execute("SELECT alias,episode_id FROM terminal_truth_alias ORDER BY alias"):
                aliases_by_id.setdefault(str(alias_row["episode_id"]), []).append(str(alias_row["alias"]))
            out = []
            for row in rows:
                payload = _row_to_dict(row)
                payload["episode_aliases"] = aliases_by_id.get(str(row["episode_id"]), [str(row["episode_id"])])
                out.append(payload)
            return out
        finally:
            conn.close()


def pending_terminal_projections(*, path: Optional[Path] = None, limit: int = 128) -> list[Dict[str, Any]]:
    target = Path(path or terminal_truth_db_path())
    if not target.exists():
        return []
    lock = _path_lock(target)
    with lock:
        conn = _connect(target)
        try:
            rows = conn.execute(
                """SELECT * FROM terminal_projection_outbox
                     WHERE status!='ACKED'
                     ORDER BY terminal_at,episode_id LIMIT ?""",
                (max(1, int(limit or 128)),),
            ).fetchall()
            return [_projection_row_to_dict(row) for row in rows]
        finally:
            conn.close()


def terminal_projection_for_episode(episode_id: str, *, path: Optional[Path] = None) -> Dict[str, Any]:
    eid = str(episode_id or "").strip()
    if not eid:
        return {}
    target = Path(path or terminal_truth_db_path())
    if not target.exists():
        return {}
    keys = _expanded_aliases(eid)
    lock = _path_lock(target)
    with lock:
        conn = _connect(target)
        try:
            canonical = _find_existing(conn, keys)
            canonical_id = str(canonical["episode_id"] or "") if canonical is not None else _preferred_canonical_id(keys)
            row = conn.execute("SELECT * FROM terminal_projection_outbox WHERE episode_id=?", (canonical_id,)).fetchone()
            return _projection_row_to_dict(row) if row is not None else {}
        finally:
            conn.close()


def acknowledge_terminal_projection(
    episode_id: str, *, acknowledged_at: str = "", path: Optional[Path] = None,
) -> Dict[str, Any]:
    target = Path(path or terminal_truth_db_path())
    if not target.exists():
        return {"acknowledged": False, "reason": "TERMINAL_TRUTH_DB_MISSING"}
    keys = _expanded_aliases(str(episode_id or ""))
    lock = _path_lock(target)
    with lock:
        conn = _connect(target)
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = _find_existing(conn, keys)
            if existing is None:
                conn.execute("ROLLBACK")
                return {"acknowledged": False, "reason": "TERMINAL_TRUTH_MISSING"}
            canonical_id = str(existing["episode_id"] or "")
            projection = conn.execute(
                "SELECT status,acknowledged_at FROM terminal_projection_outbox WHERE episode_id=?",
                (canonical_id,),
            ).fetchone()
            if projection is None:
                conn.execute("ROLLBACK")
                return {"acknowledged": False, "reason": "TERMINAL_PROJECTION_MISSING", "episode_id": canonical_id}
            if str(projection["status"] or "").upper() == "ACKED":
                conn.execute("COMMIT")
                return {
                    "acknowledged": True, "idempotent": True, "episode_id": canonical_id,
                    "acknowledged_at": str(projection["acknowledged_at"] or ""),
                }
            when = str(acknowledged_at or _utc_now())
            cur = conn.execute(
                """UPDATE terminal_projection_outbox
                      SET status='ACKED',acknowledged_at=?,updated_at=?,last_error=''
                    WHERE episode_id=? AND status!='ACKED'""",
                (when, when, canonical_id),
            )
            conn.execute("COMMIT")
            return {
                "acknowledged": bool(cur.rowcount), "idempotent": False,
                "episode_id": canonical_id, "acknowledged_at": when,
            }
        except Exception as exc:
            rollback_error = ""
            try:
                conn.execute("ROLLBACK")
            except Exception as rollback_exc:
                rollback_error = f":ROLLBACK_FAILED:{type(rollback_exc).__name__}:{rollback_exc}"
            raise TerminalTruthPersistenceError(
                f"TERMINAL_PROJECTION_ACK_FAILED:{type(exc).__name__}:{exc}{rollback_error}"
            ) from exc
        finally:
            conn.close()


def record_terminal_projection_failure(
    episode_id: str, error: str, *, at: str = "", path: Optional[Path] = None,
) -> Dict[str, Any]:
    target = Path(path or terminal_truth_db_path())
    if not target.exists():
        return {"recorded": False, "reason": "TERMINAL_TRUTH_DB_MISSING"}
    keys = _expanded_aliases(str(episode_id or ""))
    lock = _path_lock(target)
    with lock:
        conn = _connect(target)
        try:
            conn.execute("BEGIN IMMEDIATE")
            existing = _find_existing(conn, keys)
            if existing is None:
                conn.execute("ROLLBACK")
                return {"recorded": False, "reason": "TERMINAL_TRUTH_MISSING"}
            canonical_id = str(existing["episode_id"] or "")
            when = str(at or _utc_now())
            cur = conn.execute(
                """UPDATE terminal_projection_outbox
                      SET attempts=attempts+1,last_error=?,updated_at=?
                    WHERE episode_id=? AND status!='ACKED'""",
                (str(error or "TERMINAL_PROJECTION_OWNER_DID_NOT_ACKNOWLEDGE"), when, canonical_id),
            )
            conn.execute("COMMIT")
            return {"recorded": bool(cur.rowcount), "episode_id": canonical_id, "at": when}
        except Exception as exc:
            rollback_error = ""
            try:
                conn.execute("ROLLBACK")
            except Exception as rollback_exc:
                rollback_error = f":ROLLBACK_FAILED:{type(rollback_exc).__name__}:{rollback_exc}"
            raise TerminalTruthPersistenceError(
                f"TERMINAL_PROJECTION_FAILURE_RECORD_FAILED:{type(exc).__name__}:{exc}{rollback_error}"
            ) from exc
        finally:
            conn.close()


def pending_terminal_projection_count(*, path: Optional[Path] = None) -> int:
    target = Path(path or terminal_truth_db_path())
    if not target.exists():
        return 0
    lock = _path_lock(target)
    with lock:
        conn = _connect(target)
        try:
            row = conn.execute("SELECT COUNT(*) FROM terminal_projection_outbox WHERE status!='ACKED'").fetchone()
            return int(row[0] or 0) if row is not None else 0
        finally:
            conn.close()


__all__ = [
    "VERSION", "PROJECTION_CONTRACT", "TerminalTruthError", "TerminalTruthConflict",
    "TerminalTruthPersistenceError", "terminal_truth_db_path", "commit_terminal_truth",
    "terminal_truth_for_episode", "load_terminal_truths", "pending_terminal_projections",
    "terminal_projection_for_episode", "acknowledge_terminal_projection",
    "record_terminal_projection_failure", "pending_terminal_projection_count",
    "prepare_terminal_transition_intent", "clear_terminal_transition_intent",
    "pending_terminal_transition_intents", "reconcile_terminal_transition_intents",
]
