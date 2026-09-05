# R4_F33 Current Review Authorities

These modules expose current R4/F32/F33 authority seams that materially affect the signal journey and were not represented by the original H8 public mirror.

They are published for code review, not to make this repository runnable.

Key boundaries:
- US Probability resolution is parent-bound and fail-closed;
- non-US Probability IPC remains compatible with inherited worker signatures;
- volatile Probability IPC is session-isolated;
- source leases are conserved and observable;
- sealed probability requires exact source-close lineage;
- late Probability results receive only a current-price execution guard and do not rewrite the immutable model anchor;
- market/session authority is explicit by family.

If a finding depends on a large production module marked historical in `R4_F33_MIRROR_STATUS.md`, validate it against the Canonical Exact before classifying it as a current production defect.
