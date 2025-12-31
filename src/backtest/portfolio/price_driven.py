# src/backtest/portfolio/price_driven.py
from __future__ import annotations
from typing import List

from src.backtest.events import MarketEvent, SignalEvent
from src.backtest.portfolio.base import Portfolio


class PriceDrivenPortfolio(Portfolio):
    """
    Price-Driven Portfolio（FINAL）

    数学语义：
      PnL(t+1) = position(t) × (price(t+1) − price(t))
    """

    def __init__(self):
        self._position: float = 0.0
        self._last_price: float | None = None

        self._equity: float = 0.0
        self._records: List[tuple[int, float]] = []
        self._n_signals: int = 0

    # ---------------------------------
    def on_signal(self, signal: SignalEvent):
        self._position = signal.direction * signal.strength
        self._n_signals += 1

    def on_fill(self, fill):
        raise RuntimeError("PriceDrivenPortfolio must not receive FillEvent")

    def on_market(self, event: MarketEvent):
        price = event.price

        if self._last_price is not None:
            pnl = self._position * (price - self._last_price)
            self._equity += pnl
            self._records.append((event.ts, self._equity))

        self._last_price = price

    # ---------------------------------
    # 🔒 Frozen outputs
    @property
    def equity(self) -> float:
        return self._equity

    @property
    def equity_curve(self) -> list[float]:
        return [v for _, v in self._records]

    @property
    def timestamps(self) -> list[int]:
        return [ts for ts, _ in self._records]

    @property
    def n_signals(self) -> int:
        return self._n_signals
