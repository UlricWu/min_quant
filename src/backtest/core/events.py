from __future__ import annotations
from dataclasses import dataclass
from enum import Enum

# src/backtest/core/events.py
class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class Order:
    symbol: str
    side: Side
    qty: int
    ts_us: int


@dataclass(frozen=True)
class Fill:
    symbol: str
    side: Side
    qty: int
    price: float
    ts_us: int
