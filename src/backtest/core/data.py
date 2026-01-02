from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional

# src/backtest/core/data.py
class MarketDataView(ABC):
    """
    World-facing data interface.

    Answers ONE question:
    - At time t, what facts are observable?
    """

    @abstractmethod
    def on_time(self, ts_us: int) -> None:
        """Advance internal state to time ts_us"""

    @abstractmethod
    def get_price(self, symbol: str) -> Optional[float]:
        """Observable price at current time"""

    @abstractmethod
    def get_features(self, symbol: str) -> Dict[str, Any]:
        """
        Observable feature snapshot.

        Engine may choose not to expose features.
        """
