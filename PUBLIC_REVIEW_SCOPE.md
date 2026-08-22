# Public Review Scope

## Included authority path

`Source DAT generation`
→ `source read/freshness/generation identity`
→ `durable source observation`
→ `pulse/cross state`
→ `probability stage boundary`
→ `P50/P100 adapter (redacted model implementation)`
→ `pulse birth / sealed transition`
→ `R1 lifecycle`
→ `opportunity/decision policy`
→ `R50/R100/terminal authority`
→ `historical seal / durable projections`
→ `UI delivery outbox`

## Redaction boundary
Only the statistical model implementation is replaced. The surrounding code remains reviewable so the reviewer can verify:
- when the model is called;
- what lineage must accompany the call;
- how a missing/unavailable model fails;
- how P50/P100 are admitted and frozen;
- how R1, R50/R100 and terminal truth consume the result;
- how projection/UI are downstream of durable truth.

## Not a production artifact
Imports outside this folder may intentionally be unresolved. This mirror is for architectural and causal review, not execution.
