# -*- coding: utf-8 -*-
"""Prepare legal identity/provenance for append-only GANN20 episode events."""
from __future__ import annotations

from typing import Any, Dict, Mapping

from live_episode_truth_contract import (
    TRUTH_LIFECYCLE, TRUTH_LIVE_SOURCE, TRUTH_LIVE_TICK, TRUTH_R1_ACTIVATION,
    TRUTH_SEALED, TRUTH_TERMINAL, stamp_truth, validate_truth,
)

VERSION = "V86CL_R169_3_6_9_GANN20_EVENT_TRUTH_WRITER_V1"
_TERMINAL_HINTS = {
    "PRE_ENTRY_STOP_INVALIDATED", "STOP_REACHED", "LIVE_FAILED",
    "EXPIRED_SESSION_CLOSE", "SEALED_REJECTED", "TARGET_CONSUMED_BEFORE_ENTRY",
    "TARGET_CONSUMED_BEFORE_OFFICIAL",
}


def _truth_source(event_type: str, source: Mapping[str, Any], extra: Mapping[str, Any]) -> str:
    explicit = str(source.get("truth_source") or extra.get("truth_source") or "").strip().upper()
    if explicit:
        return explicit
    event = str(event_type or "").strip().upper()
    origin = str(extra.get("event_origin") or source.get("event_origin") or source.get("live_sniper_event_origin") or "").strip().upper()
    state = str(source.get("gann20_episode_state") or source.get("live_pulse_seal_state") or source.get("action_state") or "").strip().upper()
    if event in _TERMINAL_HINTS or state in _TERMINAL_HINTS:
        return TRUTH_TERMINAL
    if event in {"LIVE_R1_BORN", "LIVE_R1_UPDATED"}:
        return TRUTH_R1_ACTIVATION
    if "SEALED" in event or "SEALED" in state or origin == "SEALED_CROSS_BAR":
        return TRUTH_SEALED
    if origin == "LIVE_PRICE_TICK":
        return TRUTH_LIVE_TICK
    if origin == "LIVE_SOURCE_OBSERVATION":
        return TRUTH_LIVE_SOURCE
    return TRUTH_LIFECYCLE


def _origin(truth_source: str) -> str:
    source = str(truth_source or "").strip().upper()
    if source == TRUTH_SEALED:
        return "SEALED_CROSS_BAR"
    if source in {TRUTH_LIVE_TICK, TRUTH_R1_ACTIVATION, TRUTH_TERMINAL}:
        return "LIVE_PRICE_TICK"
    if source == TRUTH_LIVE_SOURCE:
        return "LIVE_SOURCE_OBSERVATION"
    return "LIFECYCLE"


def prepare(event_type: str, row: Mapping[str, Any], extra: Mapping[str, Any], *, producer_version: str) -> Dict[str, Any]:
    event_extra = dict(extra or {})
    truth_source = _truth_source(event_type, row or {}, event_extra)
    source = stamp_truth(row or {}, truth_source=truth_source, producer_contract_version=producer_version)
    valid, error = validate_truth(source)
    episode_id = str(source.get("pulse_episode_id") or source.get("episode_id") or source.get("id") or "").strip()
    event_origin = str(event_extra.get("event_origin") or source.get("event_origin") or source.get("live_sniper_event_origin") or _origin(truth_source)).strip().upper()
    return {
        "source": source, "extra": event_extra, "episode_id": episode_id,
        "market": str(source.get("market_key") or source.get("decision_market_key") or ""),
        "symbol": str(source.get("symbol") or "").strip().upper(),
        "event_origin": event_origin,
        "error": "" if valid else error,
    }


def payload_fields(source: Mapping[str, Any], event_origin: str, episode_id: str) -> Dict[str, Any]:
    return {
        "episode_id": source.get("episode_id") or episode_id,
        "episode_key": source.get("episode_key"),
        "episode_key_sha256": source.get("episode_key_sha256"),
        "truth_source": source.get("truth_source"),
        "truth_rank": source.get("truth_rank"),
        "truth_stamped_at": source.get("truth_stamped_at"),
        "producer_contract_version": source.get("producer_contract_version"),
        "event_origin": event_origin,
    }


__all__ = ["VERSION", "prepare", "payload_fields"]
