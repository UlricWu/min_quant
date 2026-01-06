from __future__ import annotations

from src.utils.datetime_utils import DateTimeUtils
import math
from typing import Dict, List, Optional

from src import logs
from src.backtest.core.events import Fill, Side, Order
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

    def execute(self, ts_us: int, orders: List[Order]) -> List[Fill]:
        fills: List[Fill] = []

        for o in orders:
            px = self.data_view.get_price(o.symbol)

            date = DateTimeUtils.parse(ts_us)
            if px is None or (isinstance(px, float) and (not math.isfinite(px) or px <= 0.0)):
                logs.info(f"[Execution] skip invalid price symbol={o.symbol} price={px} date={date} ts={ts_us}")
                continue

            logs.info(f"[Execution] {o.side.value} {o.symbol} qty={o.qty} price={px} date={date} ts={ts_us}")

            fills.append(
                Fill(
                    symbol=o.symbol,
                    side=o.side,
                    qty=int(o.qty),
                    price=float(px),
                    ts_us=int(ts_us),
                )
            )

        return fills
