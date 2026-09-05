# R4_F33 Public Mirror Status

Canonical Exact SHA-256:
`30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

This file prevents a reviewer from confusing an older public-mirror copy with current R4_F33 production source.

## Existing public files verified byte-identical to R4_F33

The following important mirror files retain the same Git blob as R4_F33 and therefore remain current without republishing:

- `01_source/live_sniper_source_io.py`
- `01_source/live_sniper_source_priority.py`
- `01_source/live_source_bar_freshness.py`
- `01_source/source_generation_identity.py`
- `03_signal_probability/probability_causal_worker_bridge.py`
- `03_signal_probability/pulse_state_engine.py`
- `04_lifecycle/decision_layer.py`
- `04_lifecycle/durable_r1_lifecycle.py`
- `04_lifecycle/fresh_opportunity_queue.py`
- `04_lifecycle/gann20_episode_ledger.py`
- `04_lifecycle/gann20_episode_truth_writer.py`
- `04_lifecycle/radar_contract_policy.py`
- `04_lifecycle/radar_probability_lane_contract.py`
- `05_terminal_projection/gann20_event_truth_projection.py`
- `05_terminal_projection/live_ui_delivery_contract.py`
- `05_terminal_projection/r1_event_truth_projection.py`
- `05_terminal_projection/session_historical_seal.py`

## Existing public files whose R4_F33 bytes changed

For these, the old single-file mirror must **not** be treated as current R4 source. The complete current R4 source is published as line-preserving chunks under `08_r4_f33_current_full/<module>/`:

- `live_sniper_source_runtime.py`
- `live_source_processing_scheduler.py`
- `episode_identity_v1.py`
- `full_source_observation_recorder.py`
- `live_pulse_seal_engine.py`
- `probability_hot_path.py`
- `probability_worker_runner.py`
- `pulse_acceptance_engine.py`
- `pulse_tick_tape.py`
- `durable_ui_patch_outbox.py`
- `terminal_truth_authority.py`

`03_signal_probability/pulse_probability_stage_contract.py` is small enough to be synchronized directly to current R4_F33 in this update.

## New R4/F32/F33 authority material

`08_r4_f33_authority/` contains current small authority/identity modules that did not exist in the original H8 review layout. `07_r4_f33_review_tests/` contains selected F32/F33 and source-capacity regressions.

## Model

`03_signal_probability/gann20_probability_model.py` is intentionally a non-scoring stub. No trained model, model artifact, feature recipe or production calibration is published.

## Runtime status

This mirror is non-runnable and is not Acceptance Evidence. R4_F33 Windows Live remains a separate gate.
