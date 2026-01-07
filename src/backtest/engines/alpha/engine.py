from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Dict

from src import logs
from src.backtest.core.time import is_minute_boundary
from src.backtest.core.events import Fill, Order, Side
from src.utils.datetime_utils import DateTimeUtils
"""
{#!filepath: src/backtest/engines/alpha/engine.py}

Engine A: Alpha Backtest (FINAL / FROZEN)

Purpose:
- Validate statistical alpha under idealized execution assumptions.

Semantics:
- Time is driven by ReplayClock.
- Decisions are made at minute boundaries.
- Execution is idealized (always filled, no slippage).
- Output is a sequence of immutable Fill events.

Invariants:
- Does not know data source formats.
- Does not know model implementations.
- Does not mutate portfolio directly.
- Portfolio state changes ONLY via Fill events.

This engine answers:
"Does the alpha exist in an idealized world?"
"""


@dataclass(frozen=True)
class EquityPoint:
    ts_us: int
    equity: float


class AlphaBacktestEngine:
    """
    Engine A (Alpha Backtest)

    Contract:
    - clock yields ts_us (epoch microseconds)
    - data_view exposes observable facts at current ts
    - strategy produces target_qty at minute boundary
    - executor converts target_qty into Fill events
    - portfolio evolves only by applying fills
    Mark-to-Market Equity:
    equity = cash + sum(position[symbol] * last_price(symbol))
    """

    def __init__(self, *, clock, data_view, strategy, portfolio, executor):
        self.clock = clock
        self.data_view = data_view
        self.strategy = strategy
        self.portfolio = portfolio
        self.executor = executor
        self.equity_curve: List[EquityPoint] = []

    def run(self) -> None:
        for ts in self.clock:
            ts_us = int(ts)
            self.data_view.on_time(ts_us)

            # --------------------------------------------------
            # 🔒 Mark-to-Market Equity (FINAL)
            # --------------------------------------------------
            equity = float(self.portfolio.cash)

            for symbol, qty in self.portfolio.positions.items():
                if qty == 0:
                    continue

                px = self.data_view.get_price(symbol)
                if px is None:
                    continue
                if isinstance(px, float) and (not math.isfinite(px) or px <= 0.0):
                    continue

                equity += qty * px

            self.equity_curve.append(
                EquityPoint(ts_us=ts_us, equity=equity)
            )

            # --------------------------------------------------
            # Strategy decision (minute boundary)
            # --------------------------------------------------
            if not is_minute_boundary(ts_us):
                continue

            target_pos: Dict[str, int] = self.strategy.on_minute(
                ts_us=ts_us,
                data_view=self.data_view,
                portfolio=self.portfolio,
            )

            orders = self._targets_to_orders(
                ts_us=ts_us,
                target_pos=target_pos,
            )

            fills: List[Fill] = self.executor.execute(ts_us, orders)

            for f in fills:
                self.portfolio.apply_fill(f)

    def _targets_to_orders(self, *, ts_us: int, target_pos: Dict[str, int]) -> List[Order]:
        orders: List[Order] = []

        for symbol, tgt in target_pos.items():
            cur = int(self.portfolio.positions.get(symbol, 0))
            tgt = int(tgt)

            # 🔒 FROZEN: target - current
            delta = tgt - cur
            if delta == 0:
                continue

            side = Side.BUY if delta > 0 else Side.SELL
            qty = abs(delta)

            date = DateTimeUtils.parse(ts_us)
            # 🔒 FROZEN (execution-domain): invalid price => no trade
            px = self.data_view.get_price(symbol)
            if px is None or (isinstance(px, float) and (not math.isfinite(px) or px <= 0.0)):
                logs.info(f"[Engine] skip order invalid price symbol={symbol} price={px} date={date} ts={ts_us}")
                continue

            logs.info(
                f"[Engine] symbol={symbol} cur={cur} tgt={tgt} delta={delta} "
                f"side={side.value} qty={qty} price={px} ts={date}"
            )
            orders.append(Order(ts_us=int(ts_us), symbol=symbol, side=side, qty=qty))

        return orders
