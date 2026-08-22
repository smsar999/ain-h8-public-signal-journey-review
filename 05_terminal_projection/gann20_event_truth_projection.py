# -*- coding: utf-8 -*-
"""Compact A99 projection of sealed/R1 truth into the episode ledger.

Keeping this projection outside :func:`gann20_episode_ledger.append_event` makes
truth propagation explicit without growing the old hot ledger writer.  The
helper is pure: it neither writes files nor changes trading decisions.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, Mapping

VERSION = "A99_GANN20_EVENT_TRUTH_PROJECTION_V1"


def payload_fields(source: Mapping[str, Any], first_present: Callable[..., Any]) -> Dict[str, Any]:
    row = source or {}
    return {
        "gann20_p_r50_pct": first_present(row, "gann20_p_r50_pct", "p50_sealed", "p50_live"),
        "gann20_p_r100_pct": first_present(row, "gann20_p_r100_pct", "p100_sealed", "p100_live"),
        "gann20_probability_pct": first_present(row, "gann20_probability_pct", "gann20_p_r50_pct", "p50_sealed", "p50_live"),
        "gann20_probability_label": row.get("gann20_probability_label") or row.get("probability_label"),
        "probability_kind": row.get("probability_kind") or row.get("gann20_probability_kind"),
        "signal_bar_close": first_present(row, "signal_bar_close", "sealed_signal_bar_close", "signal_close"),
        "sealed_signal_bar_close": first_present(row, "sealed_signal_bar_close", "signal_bar_close", "signal_close"),
        "signal_close": first_present(row, "signal_close", "signal_bar_close", "sealed_signal_bar_close"),
        "signal_bar_close_state": row.get("signal_bar_close_state"),
        "signal_bar_close_source": row.get("signal_bar_close_source"),
        "signal_bar_is_sealed": row.get("signal_bar_is_sealed"),
        "signal_bar_sealed": row.get("signal_bar_sealed"),
        "sealed_close_verified": row.get("sealed_close_verified"),
        "sealed_close_source": row.get("sealed_close_source"),
        "sealed_source_bar_time": row.get("sealed_source_bar_time"),
        "sealed_close_source_observation_id": row.get("sealed_close_source_observation_id"),
        "sealed_close_verified_at": row.get("sealed_close_verified_at"),
        "sealed_close_reason_code": row.get("sealed_close_reason_code"),
        "signal_bar_sealed_at": row.get("signal_bar_sealed_at"),
        "forming_window_start": row.get("forming_window_start"),
        "forming_window_end": row.get("forming_window_end"),
        "bar_label_mode": row.get("bar_label_mode"),
        "data_state": row.get("data_state"),
        "r1_watch_mode": row.get("r1_watch_mode"),
        "r1_watch_source": row.get("r1_watch_source"),
        "r1_levels_source": row.get("r1_levels_source"),
    }


def signature_fields(payload: Mapping[str, Any]) -> Dict[str, Any]:
    row = payload or {}
    return {
        "p100_sealed": row.get("p100_sealed"),
        "signal_bar_close": row.get("signal_bar_close"),
        "gann20_probability_label": row.get("gann20_probability_label"),
    }


__all__ = ["VERSION", "payload_fields", "signature_fields"]
