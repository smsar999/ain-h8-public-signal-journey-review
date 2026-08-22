# -*- coding: utf-8 -*-
"""Qt delivery contract for durable trader-facing lifecycle rows.

The worker may mark a row EMITTED, but only the UI consumer is allowed to ACK
it after the row was actually merged into the visible caches/tables.
"""
from __future__ import annotations

from typing import Any, Iterable, Mapping

from durable_ui_patch_outbox import acknowledge, record_failure

VERSION = "A103_UI_APPLIED_ACK_V1"
DELIVERY_ID_FIELD = "_ui_delivery_id"
DELIVERY_REVISION_FIELD = "_ui_delivery_revision"


def rows_for_emit(records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in records or []:
        if not isinstance(record, Mapping) or not isinstance(record.get("row"), Mapping):
            continue
        row = dict(record.get("row") or {})
        row[DELIVERY_ID_FIELD] = str(record.get("delivery_id") or "")
        row[DELIVERY_REVISION_FIELD] = int(record.get("revision") or 0)
        rows.append(row)
    return rows


def strip_delivery_metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    clean = dict(row or {})
    clean.pop(DELIVERY_ID_FIELD, None)
    clean.pop(DELIVERY_REVISION_FIELD, None)
    return clean


def _receipt(rows: Iterable[Mapping[str, Any]]) -> tuple[list[str], dict[str, int]]:
    ids: list[str] = []
    revisions: dict[str, int] = {}
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        delivery_id = str(row.get(DELIVERY_ID_FIELD) or "")
        if not delivery_id:
            continue
        ids.append(delivery_id)
        revisions[delivery_id] = int(row.get(DELIVERY_REVISION_FIELD) or 0)
    return ids, revisions


def prepare_for_context(
    rows: Iterable[Mapping[str, Any]], *, market: str, lane: str, session_date: str | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    delivery_rows: list[dict[str, Any]] = []
    display_rows: list[dict[str, Any]] = []
    wanted_market = str(market or "")
    wanted_lane = str(lane or "radar").lower()
    wanted_session = None if session_date is None else str(session_date or "")[:10]
    for raw in rows or []:
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        row_market = str(row.get("market_key") or row.get("decision_market_key") or wanted_market)
        row_lane = str(row.get("decision_lane") or "radar").lower()
        row_session = str(row.get("session_date") or row.get("market_session_date") or row.get("trader_board_session_date") or "")[:10]
        if row_market != wanted_market or row_lane != wanted_lane:
            continue
        if wanted_session is not None and row_session and row_session != wanted_session:
            continue
        delivery_rows.append(row)
        display_rows.append(strip_delivery_metadata(row))
    return delivery_rows, display_rows


def acknowledge_applied(rows: Iterable[Mapping[str, Any]]) -> int:
    ids, revisions = _receipt(rows)
    return acknowledge(ids, expected_revisions=revisions)


def record_apply_failure(rows: Iterable[Mapping[str, Any]], error: Any) -> int:
    ids, _revisions = _receipt(rows)
    return record_failure(ids, error)


__all__ = [
    "VERSION", "DELIVERY_ID_FIELD", "DELIVERY_REVISION_FIELD",
    "rows_for_emit", "strip_delivery_metadata", "prepare_for_context",
    "acknowledge_applied",
    "record_apply_failure",
]
