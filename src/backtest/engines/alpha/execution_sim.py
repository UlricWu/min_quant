from __future__ import annotations
from typing import Dict, List
from backtest.core.events import Fill, Side

# src/backtest/engines/alpha/execution_sim.py
class ExecutionSimulator:
    """
    Idealized execution:
    - No slippage
    - Always filled
    """

    def execute(self, ts_us: int, target_qty: Dict[str, int]) -> List[Fill]:
        fills: List[Fill] = []
        for symbol, qty in target_qty.items():
            if qty == 0:
                continue
            side = Side.BUY if qty > 0 else Side.SELL
            fills.append(Fill(symbol, side, abs(qty), price=0.0, ts_us=ts_us))
        return fills
