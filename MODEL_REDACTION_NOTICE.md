# Model Redaction Notice

The real GANN20 trained model implementation and artifacts are intentionally not part of this public repository.

`03_signal_probability/gann20_probability_model.py` is a non-scoring interface stub. It exists only so an external reviewer can understand how the rest of the signal journey interacts with the model boundary.

Do not infer model quality, feature construction, calibration, trained-tree behavior or production model availability from the stub.

The review target is the surrounding causal plumbing: source identity, scheduling, durable admission, Probability request/result lineage, lifecycle, terminal authority, decision and UI projection.
