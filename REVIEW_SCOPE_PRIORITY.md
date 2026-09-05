# Review Priority

For highest value, inspect in this order:

1. source generation / stale physical revision handling;
2. scheduler + durable admission + lease conservation;
3. Probability request/result identity and F32/F33 authority boundaries;
4. episode birth/seal/terminal identity across retry/restart;
5. durable terminal truth versus decision/UI projection;
6. locks, SQLite/fsync, queues and retry amplification that can turn correctness contracts into latency failures.

The model's predictive quality is intentionally out of scope because the trained model is not published.
