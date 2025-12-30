from __future__ import annotations
from typing import Optional

from src.backtest.core.events import (
    MarketEvent,
    SignalEvent,
    OrderEvent,
    FillEvent,
)


class Strategy:
    def on_market(self, event: MarketEvent) -> Optional[SignalEvent]:
        raise NotImplementedError


class Portfolio:
    def on_signal(self, signal: SignalEvent) -> Optional[OrderEvent]:
        raise NotImplementedError

    def on_fill(self, fill: FillEvent) -> None:
        raise NotImplementedError


class ExecutionLike:
    def on_order(self, order: OrderEvent) -> Optional[FillEvent]:
        raise NotImplementedError
