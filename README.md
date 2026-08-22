# Ain AlMudharib H8 — Public Signal Journey Review Mirror

This repository folder is a **sanitized architectural/code-review mirror** of the Saudi H8 signal journey.
It is intentionally **not runnable as a production release**.

Source candidate SHA-256:
`ae2edc688800949152240c9e27564f354650173a99ea94a15d794cc7d708e91d`

## Purpose
Allow an external reviewer to inspect the real plumbing and lifecycle of a signal from source ingestion to terminal/UI projection without publishing proprietary model weights, model recipes/calibration, secrets, live session data, credentials, or execution connectors.

## Start here
1. `SIGNAL_JOURNEY_MAP.md`
2. `PUBLIC_REVIEW_SCOPE.md`
3. `01_source/`
4. `02_observation/`
5. `03_signal_probability/`
6. `04_lifecycle/`
7. `05_terminal_projection/`
8. `06_contracts/`

## Important redaction
The production `gann20_probability_model.py` is **not published**. It is replaced with a public interface stub at:
`03_signal_probability/gann20_probability_model.py`

The stub documents the integration contract only: P50/P100 inputs/outputs, model availability/fail-closed behavior, feature stage boundary, score lineage expectations, and frozen episode semantics. It contains no trained trees, weights, proprietary feature recipe, or calibration coefficients.

## What is intentionally excluded
- Trained model files / weights / trees.
- Production model configuration and calibration coefficients.
- Secrets, API keys, credentials, secret vaults.
- Broker/connectors and account configuration.
- Live session evidence and user-specific file paths.
- Historical market datasets.
- Full release tooling and acceptance packages.

## Review question
Trace whether one source observation can be lost, duplicated, delayed, rebound to the wrong bar/episode, resurrected after terminal state, or projected to UI without the corresponding durable authority.
