# -*- coding: utf-8 -*-
"""Small bridge between the isolated probability worker and A95 replay storage.

The bridge is diagnostic-only.  With capture disabled it returns the production
result and metadata unchanged.  With capture enabled it attaches only fields
listed as non-semantic by the probability performance contract.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Tuple

VERSION = "A95_CAUSAL_WORKER_BRIDGE_V1"


def _audit_fields(meta: Mapping[str, Any], snapshot_key: str) -> Dict[str, Any]:
    if not bool((meta or {}).get("causal_audit_enabled")):
        return {}
    return {
        **dict(meta or {}),
        "probability_market_snapshot_key": str(snapshot_key or ""),
        "probability_market_snapshot_id": str(snapshot_key or ""),
        "probability_snapshot_cache_key": str(snapshot_key or ""),
    }


def _attach_fields(result: Any, fields: Mapping[str, Any]) -> Any:
    if not fields or not isinstance(result, dict):
        return result
    if result and all(isinstance(value, dict) for value in result.values()):
        return {str(key): {**dict(value or {}), **dict(fields)} for key, value in result.items()}
    return {**dict(result), **dict(fields)}


@dataclass
class CausalWorkerCapture:
    metadata: Dict[str, Any]
    capture_result: Optional[Callable[..., Any]] = None

    @classmethod
    def begin(
        cls, *, mode: str, market_key: str, snapshot_key: str,
        prepared_snapshot: Any, payload: Mapping[str, Any],
    ) -> "CausalWorkerCapture":
        try:
            from probability_causal_replay_store import capture_probability_request, capture_probability_result
            metadata = dict(capture_probability_request(
                mode=str(mode), market_key=str(market_key or ""), snapshot_key=str(snapshot_key or ""),
                prepared_snapshot=prepared_snapshot, payload=dict(payload or {}),
            ) or {})
            return cls(metadata=metadata, capture_result=capture_probability_result)
        except Exception as exc:
            return cls(metadata={
                "causal_audit_enabled": True,
                "causal_audit_capture_error_type": type(exc).__name__,
                "causal_audit_capture_error_message": str(exc),
            })

    @property
    def request_id(self) -> str:
        return str(self.metadata.get("causal_audit_request_id") or "")

    def request_fields(self) -> Dict[str, Any]:
        return {"causal_audit_request_id": self.request_id}

    def capture_failure(
        self, exc: BaseException, *, snapshot_key: str,
        note_error: Callable[[str, BaseException], None],
    ) -> None:
        if not callable(self.capture_result) or not self.request_id:
            return
        try:
            self.capture_result(
                request_id=self.request_id, result=None,
                metadata={**self.metadata, "snapshot_cache_key": str(snapshot_key or "")}, error=exc,
            )
        except Exception as audit_exc:
            note_error("capture_probability_audit_failure", audit_exc)

    def complete(
        self, result: Any, metadata: Mapping[str, Any], *, snapshot_key: str,
        note_error: Callable[[str, BaseException], None],
    ) -> Tuple[Any, Dict[str, Any]]:
        meta = {**dict(metadata or {}), **dict(self.metadata or {})}
        fields = _audit_fields(self.metadata, snapshot_key)
        result = _attach_fields(result, fields)
        enqueued = False
        if callable(self.capture_result) and self.request_id:
            try:
                enqueued = bool(self.capture_result(request_id=self.request_id, result=result, metadata=meta))
            except Exception as audit_exc:
                note_error("capture_probability_audit_result", audit_exc)
        if bool(self.metadata.get("causal_audit_enabled")):
            meta["causal_audit_result_capture_enqueued"] = enqueued
            result = _attach_fields(result, {"causal_audit_result_capture_enqueued": enqueued})
        return result, meta


__all__ = ["VERSION", "CausalWorkerCapture"]
