from __future__ import annotations
from typing import Optional
from .data import MarketDataView

# src/backtest/core/pricing.py
def last_price(data: MarketDataView, symbol: str) -> Optional[float]:
    return data.get_price(symbol)
