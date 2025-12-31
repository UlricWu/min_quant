from __future__ import annotations

from src.backtest.strategy.base import Strategy
from src.backtest.events import MarketEvent, SignalEvent


class ThresholdStrategy(Strategy):
    """
    L1 Research Strategy (VALID)

    规则：
      - feature > threshold → long
      - feature < -threshold → short
      - 否则 flat
    """

    def __init__(self, feature: str, threshold: float) -> None:
        self._feature = feature
        self._threshold = threshold

    def on_market(self, event: MarketEvent) -> SignalEvent | None:
        x = event.features.get(self._feature)
        if x is None:
            return None

        if x > self._threshold:
            direction = 1
        elif x < -self._threshold:
            direction = -1
        else:
            direction = 0

        return SignalEvent(
            ts=event.ts,
            symbol=event.symbol,
            direction=direction,
            strength=1.0,
        )
# ---
# from src.backtest.strategy.factory import StrategyFactory
#
# strategy = StrategyFactory.create(strategy_cfg)