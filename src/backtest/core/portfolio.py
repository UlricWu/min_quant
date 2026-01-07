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

    def apply_fill(self, f: Fill) -> None:
        cur = int(self.positions.get(f.symbol, 0))

        if f.side.value == "BUY":
            new = cur + int(f.qty)
            self.cash -= float(f.price) * int(f.qty)
        elif f.side.value == "SELL":
            sell_qty = min(int(f.qty), cur)  # long-only: 不允许裸卖
            new = cur - sell_qty
            self.cash += float(f.price) * sell_qty
        else:
            raise ValueError(f"unknown side: {f.side}")

        self.positions[f.symbol] = new