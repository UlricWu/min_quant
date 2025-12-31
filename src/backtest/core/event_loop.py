from __future__ import annotations
from typing import Iterable

from src.backtest.core.interfaces import Strategy, Portfolio, ExecutionLike
from src.backtest.events import MarketEvent


def run_event_loop(
    *,
    data: Iterable[MarketEvent],
    strategy: Strategy,
    portfolio: Portfolio,
    execution: ExecutionLike,
) -> None:
    for event in data:
        signal = strategy.on_market(event)

        if signal is None:
            continue

        order = portfolio.on_signal(signal)

        if order is None:
            continue

        fill = execution.on_order(order)

        if fill is None:
            continue

        portfolio.on_fill(fill)
