"""
{#!filepath: src/backtest/core/__init__.py}

Core World Model (FINAL / FROZEN)

Defines WHAT the world is, independent of any experiment or engine.

Invariants:
- Time is represented as ts_us (epoch microseconds).
- Events (Order, Fill) are immutable historical facts.
- Portfolio state evolves only by applying events.
- MarketDataView defines all observable facts at time t.

Core explicitly does NOT:
- Perform IO or data loading
- Know about parquet / tick / websocket
- Contain strategy or execution logic
- Advance time by itself

All engines must strictly obey this world model.
"""
