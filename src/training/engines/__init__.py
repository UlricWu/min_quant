"""
Model Train Engines (FINAL / FROZEN)

This directory contains concrete implementations of ModelTrainEngine.

IMPORTANT:
- This is NOT a generic model abstraction layer.
- Each engine defines COMPLETE training semantics.
- Engines are NOT reusable components.
- Do NOT import these engines outside training steps.


Training Paradigms
------------------

Every ModelTrainEngine MUST explicitly belong to EXACTLY ONE
training paradigm:

1) Day-Scoped Batch Training
   - Finite dataset
   - Stateless across runs (unless explicitly reloaded)
   - fit(X, y) semantics

2) Event-Stream Online Training
   - Unbounded data stream
   - Stateful model evolution
   - partial_fit(...) semantics


Engine Responsibilities
-----------------------

A ModelTrainEngine decides:
- Regression vs classification semantics
- How labels are interpreted (never inferred)
- Whether training is batch or online
- Whether model state is ephemeral or checkpointed

A ModelTrainEngine MUST NOT:
- Guess label dtype or semantics
- Accumulate unbounded datasets in memory
- Violate its declared training paradigm


Safety Rule (HARD):
-------------------

Batch training engines MUST reject datasets that exceed
single-machine capacity.

Online training engines MUST guarantee:
- Finite numeric inputs
- NaN / ±Inf removal before model updates
- Explicit and recoverable model state


Training Guarantees
-------------------

On successful completion, every training run produces:

artifact["model"]
    A trained estimator or model state.

artifact["feature_order"]
    Feature order that MUST match inference-time expectations.

Inference assumes training correctness.
Inference does NOT re-validate training semantics.
"""
