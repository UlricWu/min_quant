# src/backtest/execution/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from src.backtest.events import OrderEvent, FillEvent


class ExecutionLike(ABC):
    @abstractmethod
    def on_order(self, order: OrderEvent) -> FillEvent | None:
        ...
