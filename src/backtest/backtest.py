# backtest/backtest.py
from __future__ import annotations
from typing import Iterable, List

from src.backtest.events import MarketEvent
from src.backtest.alpha import AlphaStrategy
from src.backtest.portfolio import Portfolio
from src.backtest.execution import ExecutionEngine


class BacktestEngine:
    def __init__(
        self,
        market_events: Iterable[MarketEvent],
        alpha: AlphaStrategy,
        portfolio: Portfolio,
        execution: ExecutionEngine,
    ):
        self.market_events = market_events
        self.alpha = alpha
        self.portfolio = portfolio
        self.execution = execution

        self.equity_curve: List[float] = []

    def run(self) -> None:
        for market in self.market_events:
            signal = self.alpha.on_market(market)
            if signal is None:
                self._mark_to_market(market)
                continue

            order = self.portfolio.on_signal(signal)
            fill = self.execution.execute(order, market)
            self.portfolio.on_fill(fill)

            self._mark_to_market(market)

    def _mark_to_market(self, market: MarketEvent) -> None:
        pos = self.portfolio.positions.get(market.symbol, 0)
        equity = self.portfolio.cash + pos * market.close
        self.equity_curve.append(equity)
