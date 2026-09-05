from __future__ import annotations

import pandas as pd

from live_monitor_engine_pkg._mixin_ranker_ml import _RankerMLMixin


def _engine_with_worker(worker, *, authority):
    engine = _RankerMLMixin.__new__(_RankerMLMixin)
    engine._get_isolated_probability_worker = lambda: worker
    engine._probability_worker_timeout_sec = lambda cold=False: 5.0
    engine._annotate_probability_worker_sla = lambda meta, **_kwargs: dict(meta or {})
    engine._probability_worker_timing_payload = lambda meta, timeout_sec: {}
    engine._h1r13_r4_parent_probability_resolution_authority = (
        lambda *_args, **_kwargs: dict(authority or {})
    )
    return engine


def test_f33_non_us_batch_preserves_inherited_worker_signature_and_canonicalization():
    class Worker:
        def health_snapshot(self):
            return {"worker_ready": True}

        # Intentionally inherited signature: no resolution_authority keyword.
        def score_batch(self, _df, _records, *, market_key, timeout_sec):
            assert market_key == "sa"
            assert timeout_sec == 5.0
            return {"abc": {"available": True, "p50": 0.3}}, {"worker_ready": True}

    engine = _engine_with_worker(Worker(), authority={})
    result = engine._score_market_probability_batch_isolated(
        pd.DataFrame(), {"ABC": []}, market_key="sa"
    )
    assert list(result) == ["ABC"]
    assert result["ABC"]["available"] is True


def test_f33_non_us_single_preserves_inherited_worker_signature():
    class Worker:
        def health_snapshot(self):
            return {"worker_ready": True}

        # Intentionally inherited signature: no resolution_authority keyword.
        def score_single(
            self, _df, _records, *, market_key, symbol, sealed, timeout_sec,
            sealed_bar_time="", sealed_source_observation_id=""
        ):
            assert market_key == "fx"
            assert symbol == "XAUUSD"
            return {"available": True, "p50": 0.4}, {"worker_ready": True}

    engine = _engine_with_worker(Worker(), authority={})
    result = engine._score_market_probability_single_isolated(
        pd.DataFrame(), [], market_key="fx", symbol="XAUUSD", sealed=False
    )
    assert result["available"] is True


def test_f33_us_batch_still_requires_parent_bound_resolution_authority_keyword():
    seen = {}

    class Worker:
        def health_snapshot(self):
            return {"worker_ready": True}

        def score_batch(
            self, _df, _records, *, market_key, timeout_sec, resolution_authority
        ):
            seen["authority"] = dict(resolution_authority)
            return {"abc": {"available": True}}, {"worker_ready": True}

    authority = {
        "contract": "H1R13_R4_F32_PARENT_BOUND_RESOLUTION_AUTHORITY_V1",
        "built_by": "PARENT",
        "market_key": "us",
        "resolution_complete": False,
        "resolution_status": "FAIL_CLOSED_TEST",
    }
    engine = _engine_with_worker(Worker(), authority=authority)
    result = engine._score_market_probability_batch_isolated(
        pd.DataFrame(), {"ABC": []}, market_key="us"
    )
    assert result["ABC"]["available"] is True
    assert seen["authority"] == authority
