# R4_F33 Public Mirror Status

Canonical Exact SHA-256:
`30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

This repository is a **sanitized R4_F33 control-flow / signal-journey review mirror**, not a byte-for-byte release mirror and not a runnable build.

The purpose of this status file is to stop a reviewer from confusing an older public copy with current R4_F33 production bytes.

## A. Existing public files verified byte-identical to R4_F33

These important files retained the same Git blob identity as the R4_F33 source and may be treated as current production-source evidence for code review:

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

`03_signal_probability/pulse_probability_stage_contract.py` was synchronized directly to its current R4_F33 source in this update.

## B. Legacy public copies whose bytes differ from R4_F33

The following paths remain useful for architectural history, but their old single-file public copies **must not be cited as current R4_F33 bytes**:

- `01_source/live_sniper_source_runtime.py`
- `01_source/live_source_processing_scheduler.py`
- `02_observation/episode_identity_v1.py`
- `02_observation/full_source_observation_recorder.py`
- `03_signal_probability/live_pulse_seal_engine.py`
- `03_signal_probability/probability_hot_path.py`
- `03_signal_probability/probability_worker_runner.py`
- `03_signal_probability/pulse_acceptance_engine.py`
- `04_lifecycle/pulse_tick_tape.py`
- `05_terminal_projection/durable_ui_patch_outbox.py`
- `05_terminal_projection/terminal_truth_authority.py`

Their old public copies are intentionally retained rather than silently rewritten and misrepresented. See `R4_F33_CHANGED_FILE_IDENTITIES.md` for old-vs-current identities.

## C. Current R4/F32/F33 material published for the review

`08_r4_f33_authority/` publishes the current review-relevant authority/identity seams, including:

- parent-bound US Probability resolution authority;
- Probability IPC path/session isolation;
- source lease accounting;
- Probability snapshot parent cache;
- sealed-close lineage;
- sealed Probability authority;
- late Probability-result price guard;
- market/session authority.

`07_r4_f33_review_tests/` publishes selected current F32/F33 regressions showing the intended US fail-closed and non-US compatibility contracts.

These files are provided to let the reviewer inspect the **current causal seams that changed after the original H8 mirror**, without publishing the full operational release.

## D. What is intentionally NOT published

- trained GANN20 model files, trees, weights or serialized artifacts;
- proprietary feature recipe and production calibration coefficients;
- secret-vault contents, API keys, credentials or account configuration;
- live-session evidence or historical market datasets;
- complete release/acceptance packages and dependency bundles.

`03_signal_probability/gann20_probability_model.py` remains an explicit non-scoring public stub.

## E. Correct use of this mirror

It is highly suitable for auditing the end-to-end signal journey, identities, queues, durable admission, Probability request/result binding, lifecycle, terminal truth and UI projection.

It is **not** sufficient to prove:
- model quality or feature/calibration correctness;
- physical Windows timing/performance;
- byte-identical behavior of every changed large production module;
- Live GO.

Any suspected defect should be carried back to the Canonical Exact and proved causally before being classified as a production Finding.
