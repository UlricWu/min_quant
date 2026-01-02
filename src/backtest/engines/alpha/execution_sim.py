from __future__ import annotations

from typing import Dict, List, Optional

from src.backtest.core.events import Fill, Side
from src.backtest.core.data import MarketDataView
"""
{#!filepath: src/backtest/engines/alpha/execution_sim.py}

ExecutionSimulator (Engine A / FINAL / FROZEN)

Role:
- Convert target_qty into immutable Fill events at a given time.

Assumptions:
- Idealized execution.
- No slippage.
- Always filled if price is observable.

Invariants:
- Does NOT advance time.
- Does NOT mutate portfolio.
- Emits ONLY immutable Fill events.

Execution mechanisms differ across engines;
event semantics NEVER change.
"""



class ExecutionSimulator:
    """
    Idealized execution (Engine A)

    Contract:
    - At time ts_us, convert target_qty into Fill events
    - Uses observable price via MarketDataView.get_price(symbol)
    - If price is unavailable -> skip that symbol (no fill)
    """

    def __init__(self, *, data_view: MarketDataView):
        self.data_view = data_view

    def execute(self, ts_us: int, target_qty: Dict[str, int]) -> List[Fill]:
        fills: List[Fill] = []

        for symbol, qty in target_qty.items():
            if qty == 0:
                continue

            price = self._resolve_price(symbol)
            if price is None:
                continue

            side = Side.BUY if qty > 0 else Side.SELL
            fills.append(
                Fill(
                    symbol=symbol,
                    side=side,
                    qty=abs(int(qty)),
                    price=float(price),
                    ts_us=int(ts_us),
                )
            )

        return fills

    def _resolve_price(self, symbol: str) -> Optional[float]:
        return self.data_view.get_price(symbol)
