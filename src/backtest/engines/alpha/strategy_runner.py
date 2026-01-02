from __future__ import annotations
from typing import Dict
from backtest.core.data import MarketDataView
from backtest.core.portfolio import Portfolio
# src/backtest/engines/alpha/strategy_runner.py

class StrategyRunner:
    """
    Strategy + Model executor.

    - Consumes observable facts only
    - Never knows data source
    """

    def __init__(self, model, strategy):
        self.model = model
        self.strategy = strategy

    def on_minute(
        self,
        ts_us: int,
        data: MarketDataView,
        portfolio: Portfolio,
    ) -> Dict[str, int]:
        features = {}  # build from data.get_features()
        y_pred = self.model.predict(features)
        return self.strategy.decide(ts_us, y_pred, portfolio)
