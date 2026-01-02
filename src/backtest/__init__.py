"""
{#!filepath: src/backtest/__init__.py}

Backtest System (FINAL / FROZEN)

This package defines a reusable backtesting framework based on a single,
strict world model and multiple execution engines.

Core doctrine:
- There is exactly ONE world model.
- Time is absolute (epoch microseconds, ts_us).
- State evolves ONLY through immutable events.
- Engines differ only in how time is driven and how events are generated.

Layer responsibilities:
- core     : defines what the world IS (time, events, state, observability)
- meta     : defines WHERE immutable facts live (symbol → slice)
- engines  : defines HOW the world is driven (replay, execution, validation)
- pipeline : defines WHEN experiments are executed

Violating these boundaries breaks correctness and reproducibility.
"""
