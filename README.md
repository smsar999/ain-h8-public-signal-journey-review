# Ain Al-Mudharib R4_F33 — Public Signal Journey Review Mirror

This repository is a **sanitized, non-runnable code-review mirror** of the current R4_F33 signal journey.
It is intended for an external expert to trace the production plumbing from physical/source observation to scheduling, durable admission, Probability, lifecycle, terminal truth, decision and UI projection.

Source Canonical Exact SHA-256:
`30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

Source release status: **STRICT PRELIVE; Formal/Full/External offline gates passed; Windows Live pending.**

## Review rule
This is a review mirror, not a runnable release. Included production modules are published only to expose control flow, identities, queues, authorities, durable truth and projection behavior. The GANN20 model implementation itself is intentionally redacted.

## Start here
1. `EXPERT_START_HERE.md`
2. `R4_F33_MIRROR_STATUS.md`
3. `R4_F33_CURRENT_FUNCTION_INDEX.md`
4. `SIGNAL_JOURNEY_MAP.md`
5. `PUBLIC_REVIEW_SCOPE.md`
6. `REVIEW_QUESTIONS.md`
7. `01_source/`
8. `02_observation/`
9. `03_signal_probability/`
10. `04_lifecycle/`
11. `05_terminal_projection/`
12. `06_contracts/`
13. `07_r4_f33_review_tests/`
14. `08_r4_f33_authority/`

## Intentionally excluded
- trained model files / trees / weights / serialized model artifacts;
- proprietary feature recipe and production calibration coefficients;
- secrets, API keys, credentials and secret-vault contents;
- broker/account connectors and account configuration;
- live-session evidence, historical market datasets and user-specific paths;
- complete release/acceptance packages and offline dependency bundles.

`03_signal_probability/gann20_probability_model.py` remains an explicit non-scoring public stub.

## Primary review objective
Trace whether one source observation can be lost, duplicated, delayed, rebound to the wrong physical generation/bar/episode, scheduled under the wrong authority, scored against stale context, resurrected after terminal state, or projected to decision/UI without matching durable truth.

## Important
A PASS in this mirror is not Live GO. The authoritative release remains the R4_F33 Canonical Exact identified above; Windows Live acceptance is still a separate gate.
