# src/backtest/strategy/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from src.backtest.events import MarketEvent, SignalEvent


class Strategy(ABC):
    """
    Strategy (FINAL / FROZEN)

    纯解释器：
      MarketEvent -> SignalEvent | None
    """

    @abstractmethod
    def on_market(self, event: MarketEvent) -> Optional[SignalEvent]:
        ...