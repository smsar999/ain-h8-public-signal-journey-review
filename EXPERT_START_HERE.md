# Expert Start Here — R4_F33 Signal Journey

If your goal is to find bugs in the signal journey, use this sequence:

1. Read `R4_F33_MIRROR_STATUS.md` so you know exactly which public files are current R4 bytes and which legacy copies are historical context only.
2. Read `SIGNAL_JOURNEY_MAP.md` for the end-to-end path.
3. Read `REVIEW_QUESTIONS.md` and challenge each authority boundary.
4. Inspect source/generation/freshness in `01_source/`.
5. Inspect observation identity and durable recording in `02_observation/`.
6. Inspect Probability plumbing in `03_signal_probability/`; the model implementation itself is intentionally a stub.
7. Inspect lifecycle/episode/seal/tape in `04_lifecycle/`.
8. Inspect durable terminal/UI projection in `05_terminal_projection/`.
9. Inspect current R4/F32/F33 authority seams in `08_r4_f33_authority/`.
10. Use `07_r4_f33_review_tests/` to understand contracts that any proposed fix must preserve.

Preferred finding format:

`OBSERVATION -> HYPOTHESIS -> CAUSAL PROOF -> IMPACT -> MINIMAL FIX -> REGRESSION TEST`

Do not promote an assumption from a fixture, old public copy, or synthetic test into a current R4 production Finding without proving the same precondition against the Canonical Exact.
