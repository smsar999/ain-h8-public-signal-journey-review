# -*- coding: utf-8 -*-
"""Parent-side mirror of the worker's bounded snapshot cache."""
from __future__ import annotations

import os
from typing import MutableSet

VERSION = "A99_PARENT_SNAPSHOT_CACHE_V1"


def cache_limit() -> int:
    try:
        return max(1, min(16, int(os.environ.get("AIN_PROBABILITY_SNAPSHOT_CACHE_MAX", "4") or 4)))
    except (TypeError, ValueError, OverflowError):
        return 4


def remember(keys: MutableSet[str], snapshot_key: str) -> None:
    key = str(snapshot_key or "").strip()
    if not key:
        return
    if key not in keys and len(keys) >= cache_limit():
        # A set cannot represent worker LRU order.  Clearing is conservative:
        # it may cause a full send, but can never cause a stale key-only request.
        keys.clear()
    keys.add(key)


__all__ = ["VERSION", "cache_limit", "remember"]
