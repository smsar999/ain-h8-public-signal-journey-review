# R4_F33 Changed-File Identities

Canonical Exact SHA-256:
`30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

The original H8 public mirror predates R4_F33. The table below records the Git blob identity of the old public copy and the Git blob identity computed from the corresponding current R4_F33 source bytes.

A mismatch means: **do not cite the old public file as current R4 source**.

| Public path | Old public blob | R4_F33 source blob | Current in old path? |
|---|---|---|---|
| `01_source/live_sniper_source_runtime.py` | `6d10d675b9974c4115af91e21fd456b0ac4d3b34` | `a33e6307ed0129594b77a19cc772dd9e5ab84d6e` | NO |
| `01_source/live_source_processing_scheduler.py` | `abafd261ecb1913f492b8aedba82a15dd84765a9` | `faf205b2fc0d7d18d39310d53139ad2df2f4c2f5` | NO |
| `02_observation/episode_identity_v1.py` | `c925b6b6a9cfdd5960725cd127b8234632240739` | `3757c2ca4b6039ee2f61647f11625a17639027b4` | NO |
| `02_observation/full_source_observation_recorder.py` | `bb273a1b6e5b15d77af871fd336e57a86ebfc133` | `b95501a3cecb642eb107687a5b37c98d25023967` | NO |
| `03_signal_probability/live_pulse_seal_engine.py` | `4a21a596aea9140c79ce7933f02eba4c744a2ad4` | `a2410f98ef7cdaa01d821e9f2586e0a7c882a4b4` | NO |
| `03_signal_probability/probability_hot_path.py` | `e9ccd364a8d23b2bc91c4adb3926a18fba0e2eec` | `a5bbdf0b3706831b842be83d662cbe82ebed926d` | NO |
| `03_signal_probability/probability_worker_runner.py` | `502cfd21ce1cc28dcc7c82d7f783cd5bfdca838e` | `45906b49b6a565bf08d4a3d60af3e82fa44a69bd` | NO |
| `03_signal_probability/pulse_acceptance_engine.py` | `caf67aca06317a60397f72e94a512ad038ab2d48` | `19654f5517a8958a509f36918854d1652676623c` | NO |
| `03_signal_probability/pulse_probability_stage_contract.py` | `cd8c2833806d56debfbef1cf26771b83a0ebdb62` | `c565c30289e99f071d6ec66700ae9e02a5fa4cdf` | **UPDATED TO R4** |
| `04_lifecycle/pulse_tick_tape.py` | `b7448c6fcbfb3140ddea2d53cfcd8aff774e2941` | `e17993ef1611a0a9d255de7964f7798eefdb3780` | NO |
| `05_terminal_projection/durable_ui_patch_outbox.py` | `1a8bb07ff6459a846bd35670e4ac48c372684b28` | `cad566a381e6602d4eb03af0654672a3399f6a1e` | NO |
| `05_terminal_projection/terminal_truth_authority.py` | `df32207af9e73f4f6a65fc9b2cb4b6297cae3d47` | `c5330a1ea1a8efbc82add8483f3576388ad30c9c` | NO |

The Canonical Exact SHA at the top of this document is the release identity. Current review-relevant authority modules and F32/F33 regressions are published separately under `08_r4_f33_authority/` and `07_r4_f33_review_tests/`.

## Important reviewer rule

If a suspected bug depends on one of the `NO` rows above, treat the public copy as **historical context only**. Use it to understand the architecture, then validate the hypothesis against the Canonical R4_F33 source before promoting it to a production Finding.
