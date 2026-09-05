# Signal Journey Map — R4_F33

Canonical source identity:

`30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

## End-to-end path

```text
MetaStock / source file generation
  -> live_sniper_source_io / source_generation_identity
  -> live_sniper_source_runtime
  -> freshness / quarantine / resolution / supervisor
  -> full_source_observation_recorder
  -> live_source_processing_scheduler
  -> source lease + durable admission continuation
  -> engine Stage-0 / DataApi boundary
  -> Probability work partition / IPC / worker runner
  -> GANN20 model boundary (PUBLIC STUB ONLY)
  -> completion + authority + late-result guards
  -> pulse acceptance / state engine
  -> GANN20 episode ledger / seal authority
  -> pulse_tick_tape + terminal_truth_authority
  -> lifecycle / decision authority
  -> durable UI outbox / official truth / projection
```

## 1. Physical/source authority
Start in `01_source/`.

Trace:
- physical generation identity;
- partial-write/rewrite handling;
- bar freshness;
- source mapping / quarantine / resolution;
- SOURCE_PRIORITY ordering;
- construction and selection of `SourceProcessingTask`.

R4-specific source/scheduler authority additions are also under `08_r4_f33_authority/`.

## 2. Observation -> durable admission
Read `02_observation/`, then the R4 authority files for lease/admission identity.

Questions:
- Is one immutable physical observation represented once?
- Is durable debt preserved when processing cannot continue?
- Can a lease close without a corresponding open, or remain open after terminal fate?
- Can a later physical generation be rebound to an older logical episode?

## 3. Probability seam
Read `03_signal_probability/` and `08_r4_f33_authority/`.

The trained model is intentionally absent. The surrounding production path is reviewable:
- work creation;
- parent resolution authority;
- request lineage;
- IPC payload;
- worker attempt/result;
- completion accounting;
- timeout / late-result rejection;
- acceptance and state transition.

### F32 / F33 focus
- US: parent resolution authority must be non-empty and fail closed.
- non-US: inherited Probability IPC compatibility must not be broken by injecting an invalid empty authority keyword.

Selected regression tests are published in `07_r4_f33_review_tests/`.

## 4. Episode / seal lifecycle
Read `04_lifecycle/`.

Check:
- immutable episode identity;
- first-cross / sealed-score lineage;
- restart / high-water semantics;
- R1 / seal ordering;
- terminal isolation;
- radar lane side effects and stale-epoch fencing.

## 5. Terminal -> UI
Read `05_terminal_projection/`.

Invariant to challenge:
**no UI/decision projection may outrun or contradict durable terminal authority.**

Test crash, retry, SQLite contention, queue saturation, outbox failure and replay/resurrection reasoning against that invariant.

## 6. Stable-vs-current mirror identity
Read `R4_F33_MIRROR_STATUS.md` before treating any file as current. The public mirror intentionally preserves prior files only where they are byte-identical to R4_F33 or where the current R4 replacement is explicitly supplied in the R4 authority/review directories.

## 7. What this repository cannot prove
It does not prove model quality, real Windows performance, physical disk behavior, or live-market acceptance. Those remain separate evidence authorities.
