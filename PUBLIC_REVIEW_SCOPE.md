# Public Review Scope — R4_F33

This mirror is derived from R4_F33 Canonical Exact:

`30ac5a9844c5b929ba4a5616a9d0f821bba704fad445229c50a63a3cb3b025ae`

It is intentionally **not byte-identical to the release** because sensitive/model/runtime material is excluded.

## What the reviewer may treat as production control-flow evidence

Included Python modules are copied from the R4_F33 production source when marked current in `R4_F33_MIRROR_STATUS.md`. Files whose R4 bytes are unchanged from the previous public mirror remain in place with their original Git blob identity. The GANN20 model file is the one explicit implementation stub.

The purpose is to expose:
- source and physical-generation handling;
- scheduler, coalescing, reserve and same-key behavior;
- durable admission / lease accounting;
- Probability request/IPC/worker/result authority;
- episode, seal, terminal and restart identity;
- decision and UI projection boundaries.

## Reviewer must not infer
- model quality, feature correctness or calibration quality;
- Windows timing/performance from this repository alone;
- live-market success or Live GO;
- absence of a bug merely because a selected public test passes.

## Missing material by design
- weights/models and serialized model artifacts;
- secret storage and credential values;
- provider/broker/account configuration;
- live evidence and historical market datasets;
- packaging, full Court Floor and Windows acceptance bundles.

## Security rule
If an included module refers to API-key, provider or secret-vault interfaces, those are architectural references only. No secret value or vault content is intentionally published.
