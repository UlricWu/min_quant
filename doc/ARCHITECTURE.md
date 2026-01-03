ARCHITECTURE (FINAL / FROZEN)

This repository implements a unified quantitative research system
organized by domain-level systems, not by code type.

------------------------------------------------------------
Core Principle
------------------------------------------------------------

Systems define ownership.
Types do not.

Data processing, model training, and backtesting are treated as
peer systems with strict boundaries.

------------------------------------------------------------
System Decomposition
------------------------------------------------------------

The system is composed of three top-level domains:

1) Data System
   - Purpose: Produce factual data (raw → feature → label)
   - Owns: data engines, data steps
   - Output: immutable data artifacts

2) Training System
   - Purpose: Train models from features and labels
   - Owns: training engines, training steps
   - Output: versioned model artifacts

3) Backtest System
   - Purpose: Evaluate strategies and models
   - Owns: backtest engines, strategies, steps
   - Output: research results and reports

These systems are peers.
No system may call another system directly.

------------------------------------------------------------
Pipeline / Step / Engine Model
------------------------------------------------------------

- Workflow:
  Declares WHAT runs and WHEN.
  Wires pipelines only.

- Pipeline:
  Orchestrates execution order.
  Manages context and lifecycle.
  Contains no business logic.

- Step:
  Defines a semantic boundary.
  Resolves inputs and invokes engines.

- Engine:
  Executes pure business logic.
  Has no knowledge of paths, dates, or pipelines.

------------------------------------------------------------
Shared Infrastructure
------------------------------------------------------------

The following modules are shared across all systems:

- pipeline/        BasePipeline, BaseStep, Context
- meta/            Manifest, Slice, Symbol resolution
- utils/           Pure utilities
- observability/   Metrics, timeline, logging

Shared code must be system-agnostic.

------------------------------------------------------------
Backtest Design Invariant
------------------------------------------------------------

- core/ defines the observable world and time semantics
- engines/ define how the world is driven
- strategies decide actions from observable facts
- execution validates feasibility

Model inference and strategy logic must never perform I/O.

------------------------------------------------------------
Training Design Invariant
------------------------------------------------------------

- Training is not a sub-step of backtesting
- Training produces model artifacts only
- Backtesting consumes model artifacts only

Training and backtesting must remain decoupled.

------------------------------------------------------------
Forbidden Patterns
------------------------------------------------------------

- A step importing another system's step
- An engine performing file or path resolution
- A strategy or model performing I/O
- Training logic inside backtest pipelines
- Backtest logic inside data pipelines

------------------------------------------------------------
Summary
------------------------------------------------------------

Systems are isolated.
Pipelines orchestrate.
Steps define boundaries.
Engines execute.
Artifacts connect systems.

This architecture is intentionally explicit and restrictive
to preserve long-term clarity and correctness.

```python
src/
├── data/
│   ├── engines/
│   ├── steps/
│   ├── pipeline.py
│   └── __init__.py
│
├── training/
│   ├── engines/
│   ├── steps/
│   ├── pipeline.py
│   └── __init__.py
│
├── backtest/
│   ├── core/
│   ├── engines/
│   ├── steps/
│   ├── strategy/
│   ├── pipeline.py
│   └── __init__.py
│
├── meta/
├── utils/
├── observability/
├── pipeline/          # 基础设施（BasePipeline / BaseStep）
└── workflows/

```