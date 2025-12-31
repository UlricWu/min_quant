# src/backtest/portfolio/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from src.backtest.events import SignalEvent, OrderEvent, FillEvent, MarketEvent


class Portfolio(ABC):
    @abstractmethod
    def on_signal(self, signal: SignalEvent) -> OrderEvent | None:
        ...

    @abstractmethod
    def on_fill(self, fill: FillEvent) -> None:
        ...

    def on_market(self, event: MarketEvent) -> None:
        ...
