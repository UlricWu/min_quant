ARCHITECTURE (FINAL / FROZEN)

This repository implements a unified quantitative research system
organized by domain-level systems, not by code type.

The architecture is intentionally explicit and restrictive to
preserve long-term clarity, correctness, and resistance to drift.

============================================================
Core Principle
============================================================

Systems define ownership.
Types do not.

Data processing, model training, and backtesting are treated as
peer systems with strict, enforced boundaries.

============================================================
System Decomposition
============================================================

The system is composed of three top-level domains:

1) Data System
   - Purpose:
       Produce factual data (raw → feature → label)
   - Owns:
       data engines, data steps, data pipelines
   - Output:
       immutable data artifacts

2) Training System
   - Purpose:
       Train models from features and labels
   - Owns:
       training engines, training steps, training pipelines
   - Output:
       versioned model artifacts

3) Backtest System
   - Purpose:
       Evaluate strategies and models
   - Owns:
       backtest engines, strategies, steps, backtest pipelines
   - Output:
       research results and reports

These systems are peers.
No system may call another system directly.
Systems communicate only via persisted artifacts.

============================================================
Pipeline / Step / Engine Model
============================================================

- Workflow:
    Declares WHAT runs and WHEN.
    Wires pipelines only.
    Contains no execution logic.

- Pipeline:
    Orchestrates execution order.
    Manages lifecycle.
    Does not inspect business semantics.

- Step:
    Defines a semantic boundary.
    Resolves inputs and invokes engines.
    Owns a clearly defined responsibility.

- Engine:
    Executes pure business logic.
    Has no knowledge of paths, dates, symbols, or pipelines.
    Never performs orchestration.

============================================================
Context Design (FINAL / FROZEN)
============================================================

Context represents runtime state, not business logic.

Context objects are mutable data carriers passed through
pipelines and steps. They never execute logic.

------------------------------------------------------------
Context Layering
------------------------------------------------------------

Context is strictly layered into three levels:

1) Infrastructure Context (BaseContext)
2) System Context (Data / Training / Backtest)
3) Engine-local Context (optional)

------------------------------------------------------------
Level 1: BaseContext (Infrastructure)
------------------------------------------------------------

Location:
    src/pipeline/context.py

Rules:
- BaseContext MUST define NO dataclass fields
- BaseContext exists only as a marker / type anchor
- BaseContext carries no runtime data
- BaseContext must never include paths, dates, symbols, or config

Rationale:
- Prevents cross-system coupling
- Avoids dataclass inheritance ordering issues
- Forces explicit system ownership of state

------------------------------------------------------------
Level 2: System Context (System-Owned)
------------------------------------------------------------

Each system defines its own Context:

- DataContext        (src/data_system/context.py)
- TrainingContext    (src/training/context.py)
- BacktestContext    (src/backtest/context.py)

Rules:
- System Contexts MUST inherit from BaseContext
- All runtime fields belong here
- Fields reflect only that system’s execution semantics
- No System Context may be imported by another system

Examples of allowed fields:
- resolved paths
- dates / symbols
- config objects
- engine outputs

------------------------------------------------------------
Level 3: Engine-local Context (Optional)
------------------------------------------------------------

Rules:
- Used only inside a specific engine
- Never passed to pipelines or steps
- Never shared across systems
- Never placed under src/pipeline/

------------------------------------------------------------
Pipeline Interaction Rules
------------------------------------------------------------

- Pipeline does NOT define Context structure
- Pipeline only:
    * creates the initial System Context
    * passes Context between Steps
- Pipeline must never inspect or mutate Context fields directly

Steps:
- May read from Context
- May write ONLY fields they own semantically

============================================================
Shared Infrastructure
============================================================

The following modules are shared across all systems and must remain
system-agnostic:

- pipeline/        BasePipeline, PipelineStep, BaseContext
- meta/            Manifest, Slice, Symbol resolution
- utils/           Pure utilities
- observability/   Metrics, timeline, logging

Shared code must not contain system-specific semantics.

============================================================
Backtest Design Invariant
============================================================

- core/ defines the observable world and time semantics
- engines/ define how the world is driven
- strategies decide actions from observable facts
- execution validates feasibility

Model inference and strategy logic must never perform I/O.

============================================================
Training Design Invariant
============================================================

- Training is not a sub-step of backtesting
- Training produces model artifacts only
- Backtesting consumes model artifacts only

Training and backtesting must remain decoupled.

============================================================
Forbidden Patterns
============================================================

The following patterns are strictly forbidden:

- A step importing another system's step
- An engine performing file or path resolution
- A strategy or model performing I/O
- Training logic inside backtest pipelines
- Backtest logic inside data pipelines
- Defining fields on BaseContext
- Sharing a Context across systems

============================================================
Repository Layout (Frozen)
============================================================
```
src/
├── data_system/
│   ├── engines/
│   ├── steps/
│   ├── pipeline.py
│   └── context.py
│
├── training/
│   ├── engines/
│   ├── steps/
│   ├── pipeline.py
│   └── context.py
│
├── backtest/
│   ├── core/
│   ├── engines/
│   ├── steps/
│   ├── strategy/
│   ├── pipeline.py
│   └── context.py
│
├── meta/
├── utils/
├── observability/
├── pipeline/          # infrastructure only
└── workflows/
```

============================================================
Summary
============================================================

Systems are isolated.
Pipelines orchestrate.
Steps define boundaries.
Engines execute.
Contexts carry state.
Artifacts connect systems.

This architecture is intentionally strict to ensure
long-term maintainability and correctness.
