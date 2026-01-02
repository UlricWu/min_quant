from __future__ import annotations

from typing import Dict, Any, Sequence

from src.backtest.core.data import MarketDataView
from src.backtest.core.portfolio import Portfolio
"""
{#!filepath: src/backtest/engines/alpha/strategy_runner.py}

StrategyRunner (FINAL / FROZEN)

Role:
- Bridge between model predictions and target positions.

Invariants:
- Consumes ONLY observable facts (MarketDataView, Portfolio).
- Produces target_qty per symbol.
- Knows nothing about data sources, replay, or execution mechanics.

Strategy logic MUST remain engine-agnostic.
"""



class StrategyRunner:
    """
    StrategyRunner (Engine A)

    Contract:
    - Reads observable facts only (via MarketDataView + Portfolio)
    - Produces target_qty per symbol
    """

    def __init__(self, *, model, strategy, symbols: Sequence[str]):
        self.model = model
        self.strategy = strategy
        self.symbols = list(symbols)

    def on_minute(self, *, ts_us: int, data_view: MarketDataView, portfolio: Portfolio) -> Dict[str, int]:
        features_by_symbol: Dict[str, Dict[str, Any]] = {
            s: data_view.get_features(s) for s in self.symbols
        }

        scores: Dict[str, float] = self.model.predict(features_by_symbol)

        target_qty: Dict[str, int] = self.strategy.decide(
            ts_us=ts_us,
            scores=scores,
            portfolio=portfolio,
        )
        return target_qty
