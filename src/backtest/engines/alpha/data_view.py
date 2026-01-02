from __future__ import annotations
from typing import Dict, Any, Optional
from backtest.core.data import MarketDataView
# src/backtest/engines/alpha/data_view.py

class MinuteFeatureDataView(MarketDataView):
    """
    Data view backed by minute feature parquet.

    Implementation detail:
    - Uses meta.slice_accessor internally
    """

    def on_time(self, ts_us: int) -> None:
        pass  # fold ts <= current time

    def get_price(self, symbol: str) -> Optional[float]:
        return None

    def get_features(self, symbol: str) -> Dict[str, Any]:
        return {}
