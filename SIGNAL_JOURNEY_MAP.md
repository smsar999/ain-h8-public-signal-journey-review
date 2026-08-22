# Signal Journey Code Map

## 1 — Source truth

### `01_source/live_sniper_source_io.py`
Low-level source reading boundary and file-read behavior.

### `01_source/source_generation_identity.py`
Defines physical/content generation identity so a file rewrite is not confused with the previously acknowledged generation.

### `01_source/live_source_bar_freshness.py`
Bar-time/freshness authority.

### `01_source/live_sniper_source_priority.py`
Priority classification for source work.

### `01_source/live_source_processing_scheduler.py`
Bounded scheduling / protected processing capacity.

### `01_source/live_sniper_source_runtime.py`
Owns an already-read source generation through durable processing/ACK or requeue.

---

## 2 — Durable observation and episode identity

### `02_observation/full_source_observation_recorder.py`
Creates/records the immutable source-observation lineage used by later probability and lifecycle stages.

### `02_observation/episode_identity_v1.py`
Canonical episode identity contract.

---

## 3 — Cross, probability and pulse birth

### `03_signal_probability/pulse_state_engine.py`
Incremental RSIScaled/VAR3 pulse/cross state.

### `03_signal_probability/pulse_acceptance_engine.py`
Admission/acceptance layer for pulse candidates.

### `03_signal_probability/pulse_probability_stage_contract.py`
Defines the probability stage and authoritative fields used downstream.

### `03_signal_probability/probability_causal_worker_bridge.py`
Causal worker handoff.

### `03_signal_probability/probability_worker_runner.py`
Probability worker execution boundary.

### `03_signal_probability/probability_hot_path.py`
Hot-path probability orchestration and completion handling.

### `03_signal_probability/gann20_probability_model.py`
**PUBLIC STUB ONLY.** The real trained model implementation is deliberately omitted.

### `03_signal_probability/live_pulse_seal_engine.py`
Central live pulse/seal engine: provisional/official display semantics, probability linkage, sealed-bar transitions and lifecycle integration.

---

## 4 — R1 and decision lifecycle

### `04_lifecycle/pulse_tick_tape.py`
Carries observation lineage through price evolution, R1 events, target-consumed events and terminal intents.

### `04_lifecycle/durable_r1_lifecycle.py`
Canonical R1 state machine: R1 active/lost/regain, R50 tracking, R100 completion and terminal presentation state.

### `04_lifecycle/fresh_opportunity_queue.py`
Defines whether an episode is still an actionable opportunity and whether the target/entry window has been consumed.

### `04_lifecycle/decision_layer.py`
Decision/policy boundary.

### `04_lifecycle/radar_contract_policy.py`
Radar policy contract.

### `04_lifecycle/radar_probability_lane_contract.py`
Probability-lane authority contract.

### `04_lifecycle/gann20_episode_ledger.py`
Durable episode event ledger.

### `04_lifecycle/gann20_episode_truth_writer.py`
Writes episode truth events/projections from authoritative lifecycle state.

---

## 5 — Terminal truth, seal and UI projection

### `05_terminal_projection/terminal_truth_authority.py`
Durable/idempotent terminal authority.

### `05_terminal_projection/session_historical_seal.py`
Historical evidence epoch/seal lifecycle and durable session sealing.

### `05_terminal_projection/r1693_4_signal_close_authority.py`
Signal-close authority.

### `05_terminal_projection/gann20_event_truth_projection.py`
GANN20 event projection fields.

### `05_terminal_projection/r1_event_truth_projection.py`
R1 event projection.

### `05_terminal_projection/durable_ui_patch_outbox.py`
Durable UI delivery debt/outbox.

### `05_terminal_projection/live_ui_delivery_contract.py`
UI delivery acknowledgment/failure contract.

---

## 6 — Contracts to read alongside the path

- `06_contracts/gann20_episode_contract.py`
- `06_contracts/live_sniper_contract.py`
- `06_contracts/live_episode_truth_contract.py`
- `06_contracts/m30_sniper_birth_seal_contract.py`
- `06_contracts/sealed_close_lineage_contract.py`
- `06_contracts/normalized_gann.py`

## Suggested adversarial review order

1. Pick one `source_observation_id`.
2. Trace its generation identity and durable admission.
3. Verify probability cannot bind to another observation/bar.
4. Verify pulse/episode identity remains stable.
5. Verify R1 lifecycle cannot move backward illegally.
6. Verify R50/target-consumed removes entry actionability while episode may continue for R100.
7. Verify first terminal is durable/idempotent and survives restart.
8. Verify projection/UI cannot become the authority over the durable ledger.
