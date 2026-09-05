# -*- coding: utf-8 -*-
"""Bounded paths for volatile probability-worker IPC.

The evidence/log tree remains session-scoped.  Only request/response/heartbeat
files are moved to a short local runtime directory on Windows, where very long
release paths amplify Defender/indexer sharing locks and ``MAX_PATH`` pressure.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path
from typing import Mapping, Optional, Union

PathLike = Union[str, os.PathLike[str]]
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def bounded_session_token(value: object, *, prefix_chars: int = 24) -> str:
    """Return a stable filesystem token with a hard, predictable length."""
    raw = str(value or "").strip() or f"pid-{os.getpid()}"
    safe = _SAFE.sub("-", raw).strip("-._") or "session"
    digest = hashlib.sha256(raw.encode("utf-8", errors="replace")).hexdigest()[:12]
    return f"{safe[:max(4, int(prefix_chars))]}-{digest}"


def _expanded_path(value: object, env: Mapping[str, str]) -> Path:
    text = str(value or "").strip()
    for key, replacement in env.items():
        text = text.replace(f"%{key}%", str(replacement))
    return Path(os.path.expanduser(os.path.expandvars(text)))


def select_probability_ipc_dir(
    app_root: PathLike,
    *,
    session_dir: Optional[PathLike] = None,
    session_id: object = "",
    env: Optional[Mapping[str, str]] = None,
    windows: Optional[bool] = None,
    temp_dir: Optional[PathLike] = None,
) -> Path:
    """Select an isolated probability IPC directory.

    Priority:
    1. ``AIN_PROBABILITY_IPC_ROOT`` on every platform.
    2. A short per-user LocalAppData/TEMP path on Windows.
    3. The historical session/app tree on non-Windows systems.

    The returned path always includes a bounded session token so concurrent
    application sessions cannot share requests, responses, or heartbeats.
    """
    env_map: Mapping[str, str] = os.environ if env is None else env
    is_windows = os.name == "nt" if windows is None else bool(windows)
    inferred_session = session_id
    if not str(inferred_session or "").strip() and session_dir is not None:
        inferred_session = Path(session_dir).name
    token = bounded_session_token(inferred_session)

    override = str(env_map.get("AIN_PROBABILITY_IPC_ROOT", "") or "").strip()
    if override:
        return _expanded_path(override, env_map) / token

    if is_windows:
        base = (
            str(env_map.get("LOCALAPPDATA", "") or "").strip()
            or str(env_map.get("TEMP", "") or "").strip()
            or str(env_map.get("TMP", "") or "").strip()
        )
        root = _expanded_path(base, env_map) if base else Path(temp_dir or tempfile.gettempdir())
        return root / "AinAlMudharib" / "runtime" / "probability_ipc" / token

    if session_dir is not None:
        return Path(session_dir) / "workers" / "probability_ipc"
    return (
        Path(app_root)
        / "datainfo"
        / "live_tick_research"
        / "workers"
        / "probability_ipc"
        / token
    )
