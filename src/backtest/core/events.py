from __future__ import annotations
from dataclasses import dataclass
from typing import Dict


# -------------------------
# Base
# -------------------------
class Event:
    pass


# -------------------------
# Market
# -------------------------
@dataclass(frozen=True)
class MarketEvent(Event):
    ts: int
    symbol: str
    features_l0: Dict[str, float]
    features_l1: Dict[str, float]
    features_l2: Dict[str, float]


# -------------------------
# Signal
# -------------------------
@dataclass(frozen=True)
class SignalEvent(Event):
    ts: int
    symbol: str
    direction: int        # +1 / -1 / 0
    strength: float


# -------------------------
# Order
# -------------------------
@dataclass(frozen=True)
class OrderEvent(Event):
    ts: int
    symbol: str
    side: str             # BUY / SELL
    quantity: float
    order_type: str       # MARKET / LIMIT


# -------------------------
# Fill
# -------------------------
@dataclass(frozen=True)
class FillEvent(Event):
    ts: int
    symbol: str
    side: str
    quantity: float
    price: float
    commission: float
