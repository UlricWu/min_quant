from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
"""
{#!filepath: src/backtest/core/data.py}

MarketDataView (FINAL / FROZEN)

Defines WHAT is observable from the world at time t.

Contract:
- on_time(ts_us): advance the observable time cursor
- get_price(symbol): observable execution price at current time
- get_features(symbol): observable feature snapshot at current time

Invariants:
- Does NOT advance time on its own
- Does NOT expose data source details
- Does NOT contain strategy or execution logic

All engines MUST interact with market data exclusively via this interface.
"""


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
