# Public Sanitization Report — R4_F33 Mirror

Canonical source identity:
`30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

This repository is intentionally a **sanitized review mirror**, not the Canonical Exact.

## Explicitly excluded
- trained GANN20 model files / weights / trees / serialized artifacts;
- proprietary feature recipe and production calibration coefficients;
- secret-vault contents, API keys, credentials and account configuration;
- live-session evidence and historical market datasets;
- user-specific filesystem paths;
- complete Production/Acceptance packages and dependency bundles.

## Model boundary
`03_signal_probability/gann20_probability_model.py` is an explicit non-scoring stub. It exposes only the surrounding integration shape needed for review.

## Current-source disclosure
The original public H8 mirror was compared against R4_F33. Files verified byte-identical remain valid current review evidence. Files whose bytes changed are explicitly marked as historical/not-current in `R4_F33_MIRROR_STATUS.md` and their identities are recorded in `R4_F33_CHANGED_FILE_IDENTITIES.md`.

Current R4 review-relevant authority seams and F32/F33 regression tests have been added under:
- `08_r4_f33_authority/`
- `07_r4_f33_review_tests/`

## Public-release safety
No Live GO, model IP, secret value, live evidence or account credential is intentionally represented by this mirror. Any apparent secret/provider variable name in code is an interface/reference, not a published credential value.

## Review honesty
This mirror is optimized for end-to-end control-flow, identity, queue, durable-truth and projection review. It is not sufficient evidence for model quality, physical Windows performance, or release acceptance.
