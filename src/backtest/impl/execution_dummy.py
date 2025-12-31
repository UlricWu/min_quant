from __future__ import annotations
from typing import Optional

from src.backtest.core.interfaces import ExecutionLike
from src.backtest.events import OrderEvent, FillEvent


class DummyExecution(ExecutionLike):
    def on_order(self, order: OrderEvent) -> Optional[FillEvent]:
        return FillEvent(
            ts=order.ts,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=1.0,
            commission=0.0,
        )
