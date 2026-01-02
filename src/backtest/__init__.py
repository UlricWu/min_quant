"""
Backtest runtime system.

Design invariant:
- core defines time & data observability
- engines choose data source & driving mechanism
- strategy consumes observable facts, never data source
"""
