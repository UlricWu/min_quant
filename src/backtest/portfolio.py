# backtest/portfolio.py
from __future__ import annotations
from collections import defaultdict
from typing import Dict, Optional
from src.backtest.events import SignalEvent, OrderEvent, FillEvent


class Portfolio:
    def __init__(self, cash: float):
        self.cash = cash
        self.positions: Dict[str, int] = defaultdict(int)

    def on_signal(self, signal: SignalEvent) -> Optional[OrderEvent]:
        # Level 1：每次固定下 1 手
        qty = 1
        return OrderEvent(
            ts=signal.ts,
            symbol=signal.symbol,
            quantity=qty,
            direction=signal.direction,
        )

    def on_fill(self, fill: FillEvent) -> None:
        self.positions[fill.symbol] += fill.quantity
        self.cash -= fill.quantity * fill.price + fill.commission
