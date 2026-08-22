# -*- coding: utf-8 -*-
"""R169.3.4 exact signal-bar close and dual-session-close authority.

This module is deliberately pure and Qt-free.  It does not alter detector,
GANN20, thresholds, levels, or execution authority.  It only:

* builds a deterministic source-observation row for an exact historical bar
  that is already present in the MetaStock tail;
* fingerprints the exact source snapshot used for sealing;
* separates the model/regular-session 15:00 close from the Saudi 15:30
  official/auction close.

No nearest/latest/current-price fallback is permitted for a signal-bar close.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import math
from typing import Any, Dict, Iterable, Mapping, Optional

from column_truth_contract import exact_source_bar, wall_time_key
from market_key_contract import gann_market_family
from r159_pipeline_core import source_observation_id
from source_generation_identity import (
    records_dependency_sha256 as _generation_records_sha256,
    source_snapshot_identity as _generation_source_snapshot_id,
)

VERSION = "R169_3_4_SIGNAL_CLOSE_AUTHORITY_V1"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _finite(value: Any) -> Optional[float]:
    try:
        out = float(value)
        return out if math.isfinite(out) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _canonical_bar_payload(row: Mapping[str, Any]) -> Dict[str, Any]:
    raw_time = (
        (row or {}).get("date")
        or (row or {}).get("datetime")
        or (row or {}).get("time")
        or (row or {}).get("bar_datetime")
        or (row or {}).get("bar_time")
    )
    return {
        "bar_datetime": wall_time_key(raw_time),
        "open": _finite((row or {}).get("open")),
        "high": _finite((row or {}).get("high")),
        "low": _finite((row or {}).get("low")),
        "close": _finite((row or {}).get("close")),
        "volume": _finite((row or {}).get("volume")),
    }


def records_sha256(records: Iterable[Mapping[str, Any]]) -> str:
    """Content fingerprint for the exact decoded source tail."""
    return _generation_records_sha256(records)


def source_snapshot_id(*, source_file: Any, source_mtime_ns: Any, source_size: Any, tail_sha256: Any) -> str:
    return _generation_source_snapshot_id(
        source_file=source_file, source_mtime_ns=source_mtime_ns,
        source_size=source_size, dependency_sha256=tail_sha256,
    )


def build_exact_bar_source_observation(
    *,
    market_key: Any,
    symbol: Any,
    name: Any,
    source_bar: Mapping[str, Any],
    target_bar_time: Any,
    trigger_observation: Optional[Mapping[str, Any]] = None,
    source_records: Optional[Iterable[Mapping[str, Any]]] = None,
    observed_at: Any = None,
) -> Dict[str, Any]:
    """Materialize the exact old bar as an immutable source observation.

    The resulting ID is derived from the target bar OHLCV and the exact source
    file generation that contained it.  It is never derived from the new bar's
    observation ID.
    """
    trigger = dict(trigger_observation or {})
    exact, reason = exact_source_bar([dict(source_bar or {})], target_bar_time)
    if exact is None:
        raise ValueError(reason or "SEALED_CLOSE_EXACT_MATCH_VALIDATION_FAILED")

    target = wall_time_key(target_bar_time)
    tail_hash = _text(trigger.get("source_tail_sha256"))
    if not tail_hash and source_records is not None:
        tail_hash = records_sha256(source_records)

    now = observed_at or trigger.get("observed_at") or _dt.datetime.now(_dt.timezone.utc).isoformat()
    obs: Dict[str, Any] = {
        "market_key": _text(market_key),
        "symbol": _text(symbol).upper(),
        "name": _text(name) or _text(symbol).upper(),
        "bar_datetime": target,
        "bar_date": target[:10],
        "signal_bar_time": target,
        "open": exact.get("open"),
        "high": exact.get("high"),
        "low": exact.get("low"),
        "close": exact.get("close"),
        "volume": exact.get("volume"),
        "source_file": trigger.get("source_file") or exact.get("source_file"),
        "source_mtime": trigger.get("source_mtime") or exact.get("source_mtime"),
        "source_mtime_ns": trigger.get("source_mtime_ns") or exact.get("source_mtime_ns"),
        "source_size": trigger.get("source_size") or exact.get("source_size"),
        "source_tail_sha256": tail_hash,
        "source_snapshot_id": source_snapshot_id(
            source_file=trigger.get("source_file") or exact.get("source_file"),
            source_mtime_ns=trigger.get("source_mtime_ns") or exact.get("source_mtime_ns"),
            source_size=trigger.get("source_size") or exact.get("source_size"),
            tail_sha256=tail_hash,
        ),
        "source_detected_at": trigger.get("source_detected_at"),
        "observed_at": str(now),
        "live_sniper_source_lane": "EXACT_SEALED_BAR_RECONCILIATION",
        "source_reconciliation_kind": "EXACT_SIGNAL_BAR_MATERIALIZATION",
        "exact_source_bar_materialized": True,
        "seal_trigger_source_observation_id": trigger.get("source_observation_id"),
        "seal_trigger_bar_time": wall_time_key(trigger.get("bar_datetime")),
        "source_snapshot_before_size": trigger.get("source_size"),
        "source_snapshot_before_mtime_ns": trigger.get("source_mtime_ns"),
        "source_snapshot_before_tail_sha256": tail_hash,
        "source_snapshot_after_size": trigger.get("source_size"),
        "source_snapshot_after_mtime_ns": trigger.get("source_mtime_ns"),
        "source_snapshot_after_tail_sha256": tail_hash,
    }
    if source_records is not None:
        obs.update(session_close_truth(
            list(source_records), market_key=market_key, session_date=target[:10],
        ))
    obs["source_observation_id"] = source_observation_id(
        obs,
        market_key=_text(market_key),
        symbol=_text(symbol).upper(),
    )
    obs["final_source_observation_id"] = obs["source_observation_id"]
    return obs


def saudi_session_close_truth(records: Iterable[Mapping[str, Any]], session_date: Any) -> Dict[str, Any]:
    """Return separate regular 15:00 and official/auction 15:30 closes."""
    date_text = _text(session_date)[:10]
    if not date_text:
        return {
            "regular_session_bar_close_1500": None,
            "official_session_close_1530": None,
            "official_close_bar_time": "",
            "official_close_finality_state": "SESSION_DATE_MISSING",
        }
    regular_time = f"{date_text} 15:00:00"
    official_time = f"{date_text} 15:30:00"
    regular, regular_reason = exact_source_bar(records, regular_time)
    official, official_reason = exact_source_bar(records, official_time)
    regular_close = _finite((regular or {}).get("close"))
    official_close = _finite((official or {}).get("close"))
    if official is not None and official_close is not None:
        state = "OFFICIAL_SESSION_CLOSE_OBSERVED"
        official_bar = wall_time_key(official_time)
    elif regular is not None and regular_close is not None:
        state = "CLOSING_AUCTION_PENDING"
        official_bar = ""
    else:
        state = "REGULAR_SESSION_FORMING"
        official_bar = ""
    return {
        "regular_session_bar_close_1500": regular_close,
        "regular_session_bar_time_1500": wall_time_key(regular_time) if regular_close is not None else "",
        "official_session_close_1530": official_close,
        "official_close_bar_time": official_bar,
        "official_close_finality_state": state,
        "regular_session_close_reason_code": "" if regular is not None else regular_reason,
        "official_session_close_reason_code": "" if official is not None else official_reason,
        "official_close_used_for_model_signal": False,
        "official_close_used_for_outcomes": bool(official_close is not None),
    }


def session_close_truth(records: Iterable[Mapping[str, Any]], *, market_key: Any, session_date: Any) -> Dict[str, Any]:
    family = gann_market_family(market_key)
    if family == "sa":
        return saudi_session_close_truth(records, session_date)
    return {
        "regular_session_bar_close_1500": None,
        "official_session_close_1530": None,
        "official_close_bar_time": "",
        "official_close_finality_state": "NOT_APPLICABLE",
        "official_close_used_for_model_signal": False,
        "official_close_used_for_outcomes": False,
    }


__all__ = [
    "VERSION",
    "records_sha256",
    "source_snapshot_id",
    "build_exact_bar_source_observation",
    "saudi_session_close_truth",
    "session_close_truth",
]
