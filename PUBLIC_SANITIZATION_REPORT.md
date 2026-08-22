# Public Sanitization Report

## Source
Saudi H8 Windows retest candidate.
Source ZIP SHA-256:
`ae2edc688800949152240c9e27564f354650173a99ea94a15d794cc7d708e91d`

## Included
Selected production plumbing/lifecycle modules covering source ingestion, source-observation lineage, pulse/probability orchestration, R1 lifecycle, opportunity state, terminal truth, historical seal, and UI delivery contracts.

## Explicitly redacted/excluded
- Real `gann20_probability_model.py` implementation.
- Trained model artifacts and production model configuration.
- Proprietary model feature recipe and calibration values.
- Secret vault / credentials / API keys.
- Broker or account connectors.
- Live-session evidence and historical datasets.
- User-specific filesystem paths.

## Model replacement
The real model module is replaced by a non-scoring public stub that preserves the integration function names used by the surrounding code. It deliberately fails if asked to score.

## Static checks performed on the selected mirror
- Python `compileall`: PASS.
- High-confidence secret-pattern scan over selected text files: no matching credentials found.
- Explicit model artifacts: absent.

This is a review mirror, not a claim that arbitrary future edits are safe for public release.
