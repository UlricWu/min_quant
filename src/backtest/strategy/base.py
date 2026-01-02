#!filepath: src/backtest/strategy/base.py
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseModel(ABC):
    """
    Frozen interface for all models (ML / DL / rules).

    Contract:
    - Stateless at predict-time
    - Accepts feature dict per symbol
    """

    @abstractmethod
    def predict(self, features_by_symbol: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
        """
        Return per-symbol score or signal.
        """


class BaseStrategy(ABC):
    """
    Frozen interface for strategy / decision logic.

    Contract:
    - Converts model outputs into target_qty
    """

    @abstractmethod
    def decide(
        self,
        *,
        ts_us: int,
        scores: Dict[str, float],
        portfolio,
    ) -> Dict[str, int]:
        """
        Return target_qty per symbol.
        """
