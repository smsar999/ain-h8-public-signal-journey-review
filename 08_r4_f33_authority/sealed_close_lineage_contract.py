# -*- coding: utf-8 -*-
"""Exact source-close lineage carried from seal materialization to consumers."""
from __future__ import annotations

from typing import Any, Dict, Mapping

VERSION = "A96_SEALED_CLOSE_LINEAGE_CONTRACT_V1"


def sealed_close_audit_fields(
    final_observation: Mapping[str, Any], *, bar_time: Any, verified_at: Any,
) -> Dict[str, Any]:
    source = final_observation or {}
    source_id = (
        source.get("sealed_close_source_observation_id")
        or source.get("final_source_observation_id")
        or source.get("source_observation_id")
    )
    return {
        "sealed_close_verified": True,
        "sealed_close_reason_code": "",
        "sealed_close_source": source.get("sealed_close_source") or "EXACT_SOURCE_BAR",
        "sealed_source_bar_time": source.get("sealed_source_bar_time") or str(bar_time or ""),
        "sealed_close_source_observation_id": source_id or None,
        "sealed_close_verified_at": source.get("sealed_close_verified_at") or str(verified_at or ""),
        "source_read_stable": source.get("source_read_stable", True),
    }


def sealed_close_lineage_row_fields(audit: Mapping[str, Any]) -> Dict[str, Any]:
    source = audit or {}
    return {
        "sealed_source_bar_time": source.get("sealed_source_bar_time"),
        "sealed_close_verified": bool(source.get("sealed_close_verified")),
        "sealed_close_source": source.get("sealed_close_source") or "",
        "sealed_close_source_observation_id": source.get("sealed_close_source_observation_id") or None,
        "sealed_close_verified_at": source.get("sealed_close_verified_at") or None,
        "sealed_close_reason_code": source.get("sealed_close_reason_code") or "",
    }


__all__ = ["VERSION", "sealed_close_audit_fields", "sealed_close_lineage_row_fields"]
