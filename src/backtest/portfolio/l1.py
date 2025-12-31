# src/backtest/portfolio/l1.py
from __future__ import annotations

from src.backtest.portfolio.base import Portfolio
from src.backtest.events import SignalEvent, MarketEvent


class L1Portfolio(Portfolio):
    """
    L1 Portfolio (FINAL / FROZEN)

    数学语义：
      PnL(t+1) = signal(t) × label(t+1)

    事实语义：
      - equity 是累积事实
      - 时间序列由 MarketEvent.ts 决定
    """

    def __init__(self) -> None:
        self._pending: SignalEvent | None = None
        self._equity: float = 0.0

        # 🔒 内部事实（冻结）
        self._records: list[tuple[int, float]] = []
        self._n_signals: int = 0

    # --------------------------------------------------
    # Event handlers
    # --------------------------------------------------
    def on_signal(self, signal: SignalEvent):
        self._pending = signal
        self._n_signals += 1
        return None  # L1 不生成 Order

    def on_fill(self, fill):
        raise RuntimeError("L1 portfolio must not receive FillEvent")

    def on_market(self, event: MarketEvent) -> None:
        if self._pending is None:
            return

        if event.label is None:
            return

        pnl = (
            self._pending.direction
            * self._pending.strength
            * event.label
        )

        self._equity += pnl
        self._records.append((event.ts, self._equity))

        self._pending = None

    # --------------------------------------------------
    # 🔒 Research / Backtest outputs (FROZEN INTERFACE)
    # --------------------------------------------------
    @property
    def equity(self) -> float:
        return self._equity

    @property
    def equity_curve(self) -> list[float]:
        """
        仅 equity 序列（供 Metrics / Plot 使用）
        """
        return [v for _, v in self._records]

    @property
    def timestamps(self) -> list[int]:
        """
        时间事实（供 Result / Replay 对齐）
        """
        return [ts for ts, _ in self._records]

    @property
    def n_signals(self) -> int:
        return self._n_signals
