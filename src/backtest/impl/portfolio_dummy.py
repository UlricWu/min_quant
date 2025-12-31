from __future__ import annotations
from typing import Optional

from src.backtest.core.interfaces import Portfolio
from src.backtest.events import SignalEvent, OrderEvent, FillEvent


class DummyPortfolio(Portfolio):
    def on_signal(self, signal: SignalEvent) -> Optional[OrderEvent]:
        return OrderEvent(
            ts=signal.ts,
            symbol=signal.symbol,
            side="BUY",
            quantity=1.0,
            order_type="MARKET",
        )

    def on_fill(self, fill: FillEvent) -> None:
        print(f"[PORTFOLIO] filled: {fill}")
