# src/backtest/engine.py
from __future__ import annotations

from typing import Iterable

from src.backtest.events import (
    MarketEvent,
    SignalEvent,
    OrderEvent,
    FillEvent,
)
from src.backtest.strategy.base import Strategy
from src.backtest.portfolio.base import Portfolio
from src.backtest.execution.base import ExecutionLike


class BacktestEngine:
    """
    BacktestEngine (FROZEN)

    统一事件循环：
        Market → Signal → Order → Fill
    """

    def __init__(
        self,
        *,
        strategy: Strategy,
        portfolio: Portfolio,
        execution: ExecutionLike | None = None,
    ) -> None:
        self._strategy = strategy
        self._portfolio = portfolio
        self._execution = execution

    def run(self, events: Iterable[MarketEvent]) -> None:
        for event in events:
            signal: SignalEvent | None = self._strategy.on_market(event)

            if signal is not None:
                order: OrderEvent | None = self._portfolio.on_signal(signal)

                if order is not None and self._execution is not None:
                    fill: FillEvent | None = self._execution.on_order(order)
                    if fill is not None:
                        self._portfolio.on_fill(fill)

            # L1 / L2 / L3 都允许 Portfolio 感知 Market
            self._portfolio.on_market(event)
