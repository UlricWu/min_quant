"""
Core World Model (FINAL / FROZEN)

Defines WHAT the world is, independent of any experiment, engine, or pipeline.

Invariants:
- Time is represented as ts_us (epoch microseconds).
- Events (Order, Fill) are immutable historical facts.
- Portfolio state evolves ONLY by applying events.
- MarketDataView defines all observable facts at time t.

Core explicitly does NOT:
- Perform IO or data loading
- Know about parquet / tick / websocket
- Contain strategy, training, or execution logic
- Decide how or when time advances

Time advancement is always external.
All engines MUST strictly obey this world model.
"""
