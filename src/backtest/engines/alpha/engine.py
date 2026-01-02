from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict

from src.backtest.core.time import is_minute_boundary
from src.backtest.core.events import Fill


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
            self.data_view.on_time(ts)

            equity = float(self.portfolio.cash)
            self.equity_curve.append(EquityPoint(ts_us=int(ts), equity=equity))

            if not is_minute_boundary(int(ts)):
                continue

            target: Dict[str, int] = self.strategy.on_minute(
                ts_us=int(ts),
                data_view=self.data_view,
                portfolio=self.portfolio,
            )

            fills: List[Fill] = self.executor.execute(int(ts), target)

            for f in fills:
                self.portfolio.apply_fill(f)
