from __future__ import annotations
from typing import Optional

from src.backtest.core.interfaces import Strategy
from src.backtest.core.events import MarketEvent, SignalEvent


class DummyStrategy(Strategy):
    def on_market(self, event: MarketEvent) -> Optional[SignalEvent]:
        # 永远返回一个 signal（演示用）
        return SignalEvent(
            ts=event.ts,
            symbol=event.symbol,
            direction=1,
            strength=1.0,
        )
