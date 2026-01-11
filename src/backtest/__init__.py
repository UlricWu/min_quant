"""
Backtest System (FINAL / FROZEN)

This package defines a reusable backtesting framework based on a single,
strict world model and multiple execution engines.

Core doctrine:
- There is exactly ONE world model.
- Time is absolute (epoch microseconds, ts_us).
- State evolves ONLY through immutable events.
- Engines differ only in how time is driven and how events are generated.

Layer responsibilities:
- core     : defines WHAT the world IS (time, events, state, observability)
- meta     : defines WHERE immutable facts live (symbol → slice)
- engines  : defines HOW the world is driven (replay, execution, validation)
- pipeline : defines WHEN experiments are scheduled and executed

Backtest is NOT a data pipeline,
but it IS executed inside a Pipeline scheduler.

Pipeline:
- owns run lifecycle
- owns date iteration
- owns context and artifacts

Engine:
- owns time semantics
- owns event generation
- owns execution logic

Backtest correctness relies on Engine semantics.
Backtest reproducibility relies on Pipeline control.

Violating these boundaries breaks correctness and reproducibility.
"""
