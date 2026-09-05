# R4_F33 Current Function Index — Changed Large Modules

Canonical Exact SHA-256: `30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

This index was generated from the current R4_F33 source bytes. It does **not** republish the full changed modules; it gives the reviewer the exact current Git blob identity and the highest-value function/class line ranges for navigating hypotheses raised from historical public copies.

| Module | R4 Git blob | Current review hotspots |
|---|---|---|
| `live_sniper_source_runtime.py` | `a33e6307ed0129594b77a19cc772dd9e5ab84d6e` | `_recover_open_leases` 262-301; `_handle_unresolved_sources` 343-405; `_handle_scheduler_enqueue_rejection` 528-571; `_process_source_observation_after_read` 573-624; `run_source_lane_worker` 627-1240 |
| `live_source_processing_scheduler.py` | `faf205b2fc0d7d18d39310d53139ad2df2f4c2f5` | `source_processing_capacity_hints` 49-92; `source_processing_decision_reserve_hint` 95-111; `SourceProcessingTask` 115-170; `SourceProcessingScheduler` 173-1242; snapshot/quiesce helpers 1245-1318 |
| `episode_identity_v1.py` | `3757c2ca4b6039ee2f61647f11625a17639027b4` | `canonical_bar_times` 154-181; `EpisodeIdentityV1` 185-239; `build_episode_identity` 248-283; `_legacy_id_matches_identity` 319-391; `stamp_episode_identity` 394-438 |
| `full_source_observation_recorder.py` | `b95501a3cecb642eb107687a5b37c98d25023967` | `_durable_replay_observation_snapshot` 247-284; `FullSourceObservationRecorder` 491-3068; receipt/observation APIs 3088-3113; shutdown/compaction 3148-3193 |
| `live_pulse_seal_engine.py` | `a2410f98ef7cdaa01d821e9f2586e0a7c882a4b4` | `_linked_paac_snapshot` 308-399; `_execution_eval_for_pulse` 555-622; terminal commit/state helpers 740-1283; `LivePulseSealEngine` 1287-2934; market Probability scoring 3068-3347 |
| `probability_hot_path.py` | `a5bbdf0b3706831b842be83d662cbe82ebed926d` | `ProbabilityMarketFeatureCache` 243-363; `_score_target_rows` 377-441; market context authority 445-586; `score_batch` 589-714; `score_single` 717-742 |
| `probability_worker_runner.py` | `45906b49b6a565bf08d4a3d60af3e82fa44a69bd` | snapshot handling 147-169; `_score_batch_request` 224-251; sealed-lineage validation 254-335; `_score_single_request` 338-366; `serve` 458-596 |
| `pulse_acceptance_engine.py` | `19654f5517a8958a509f36918854d1652676623c` | `validate_paac_episode_identity` 72-92; `compute_pulse_observation` 231-382; `PulseEpisode` 407-447; `PulseAcceptanceEngine` 450-954 |
| `pulse_tick_tape.py` | `e17993ef1611a0a9d255de7964f7798eefdb3780` | terminal application/serialization 205-294; terminal veto/restore 508-861; candidate durability/lifecycle outbox 880-1605; projection scheduler 1611-1756; candidate registration 2060-2539; R1 price truth 2617-2684; `record_price_batch` path 2687-3298 |
| `durable_ui_patch_outbox.py` | `cad566a381e6602d4eb03af0654672a3399f6a1e` | canonicalization 98-137; durable load/save 180-245; enqueue/pending/ack/failure 248-340; archive/health 343-398 |
| `terminal_truth_authority.py` | `c5330a1ea1a8efbc82add8483f3576388ad30c9c` | terminal semantic payload 120-157; SQLite authority 160-303; transition intent 307-477; `commit_terminal_truth` 480-625; projection debt/ack/failure 670-814 |

## Reviewer rule
If a hypothesis depends on the implementation inside one of these changed modules, the old public file is historical context only. Use this current symbol/line map plus the published R4 authority/test material to localize the issue, then validate the decisive code path against the Canonical R4_F33 source before promoting it to a production Finding.
