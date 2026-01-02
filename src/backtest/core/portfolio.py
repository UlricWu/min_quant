from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict
from .events import Fill, Side
# src/backtest/core/portfolio.py

@dataclass
class Portfolio:
    cash: float
    positions: Dict[str, int] = field(default_factory=dict)
    pnl: float = 0.0

    def apply_fill(self, fill: Fill) -> None:
        qty = fill.qty if fill.side == Side.BUY else -fill.qty
        self.positions[fill.symbol] = self.positions.get(fill.symbol, 0) + qty
        self.cash -= qty * fill.price
