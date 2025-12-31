# src/backtest/replay/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

from src.backtest.events import MarketEvent


class ReplayPolicy(ABC):
    """
    ReplayPolicy (FROZEN)

    职责：
      - 决定 MarketEvent 的时间顺序
      - 不包含任何策略 / 交易逻辑
    """

    @abstractmethod
    def replay(self) -> Iterable[MarketEvent]:
        ...
