# backtest/events.py
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import Enum, auto


class EventType(Enum):
    MARKET = auto()
    SIGNAL = auto()
    ORDER = auto()
    FILL = auto()


@dataclass(frozen=True)
class Event:
    type: EventType


@dataclass(frozen=True)
class MarketEvent(Event):
    ts: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float

    def __init__(self, **kwargs):
        super().__init__(EventType.MARKET)
        object.__setattr__(self, "__dict__", kwargs)


@dataclass(frozen=True)
class SignalEvent(Event):
    ts: datetime
    symbol: str
    direction: int   # +1 buy, -1 sell, 0 flat

    def __init__(self, **kwargs):
        super().__init__(EventType.SIGNAL)
        object.__setattr__(self, "__dict__", kwargs)


@dataclass(frozen=True)
class OrderEvent(Event):
    ts: datetime
    symbol: str
    quantity: int
    direction: int

    def __init__(self, **kwargs):
        super().__init__(EventType.ORDER)
        object.__setattr__(self, "__dict__", kwargs)


@dataclass(frozen=True)
class FillEvent(Event):
    ts: datetime
    symbol: str
    quantity: int
    price: float
    commission: float

    def __init__(self, **kwargs):
        super().__init__(EventType.FILL)
        object.__setattr__(self, "__dict__", kwargs)
