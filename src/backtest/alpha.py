# backtest/alpha.py
from __future__ import annotations
from typing import Optional
from src.backtest.events import MarketEvent, SignalEvent


class AlphaStrategy:
    """
    Level 1 Alpha：
    - 不直接依赖 FeatureEngine
    - 用 bar 做最小示例（你未来可以接 ML）
    """

    def on_market(self, event: MarketEvent) -> Optional[SignalEvent]:
        # 极简示例：收盘价涨就做多，跌就做空
        direction = 0
        if event.close > event.open:
            direction = 1
        elif event.close < event.open:
            direction = -1

        if direction == 0:
            return None

        return SignalEvent(
            ts=event.ts,
            symbol=event.symbol,
            direction=direction,
        )
