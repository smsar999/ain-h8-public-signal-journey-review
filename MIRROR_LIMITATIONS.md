# Mirror Limitations

This repository intentionally maximizes reviewability of the signal journey without becoming a public production release.

A reviewer has strong visibility into current/unchanged core modules, current R4 authority seams, and selected current regressions. Some large modules changed between the original H8 public mirror and R4_F33; their old copies are explicitly marked historical rather than falsely presented as current bytes.

Therefore:
- findings on current byte-identical modules / published R4 authority seams can be assessed directly;
- findings that depend on a historical large-module copy must be rechecked against Canonical R4_F33;
- model internals, secret values, live evidence and Windows physical performance remain unavailable by design.
