# src/l2/events.py
from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional


class EventType(Enum):
    SNAPSHOT = 1
    ORDER_BOOK_UPDATE = 2
    TRADE = 3


@dataclass
class BaseEvent:
    symbol: str
    timestamp_ns: int  # 纳秒级时间戳
    event_type: EventType


@dataclass
class SnapshotEvent(BaseEvent):
    bid_prices: List[float]
    bid_volumes: List[int]
    ask_prices: List[float]
    ask_volumes: List[int]
    last_price: Optional[float] = None


@dataclass
class OrderBookUpdateEvent(BaseEvent):
    side: str  # "B" or "S" for bid or ask
    level: int
    price: float
    volume: int


@dataclass
class TradeEvent(BaseEvent):
    price: float
    volume: int
    amount: float
    aggressor_side: Optional[str] = None  # "B" or "S" or None
