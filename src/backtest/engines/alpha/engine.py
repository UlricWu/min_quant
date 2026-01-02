from __future__ import annotations
from backtest.core.time import ReplayClock, is_minute_boundary
from backtest.core.portfolio import Portfolio
from .strategy_runner import StrategyRunner
from .execution_sim import ExecutionSimulator
from backtest.core.data import MarketDataView
# src/backtest/engines/alpha/engine.py

class AlphaBacktestEngine:
    """
    Alpha backtest engine.

    - Driven by ReplayClock
    - Runs strategy only at minute boundary
    """

    def __init__(
        self,
        *,
        clock: ReplayClock,
        data_view: MarketDataView,
        strategy: StrategyRunner,
        portfolio: Portfolio,
        executor: ExecutionSimulator,
    ):
        self.clock = clock
        self.data = data_view
        self.strategy = strategy
        self.portfolio = portfolio
        self.executor = executor

    def run(self) -> None:
        for ts in self.clock:
            self.data.on_time(ts)

            if not is_minute_boundary(ts):
                continue

            target_qty = self.strategy.on_minute(ts, self.data, self.portfolio)
            fills = self.executor.execute(ts, target_qty)

            for fill in fills:
                self.portfolio.apply_fill(fill)
