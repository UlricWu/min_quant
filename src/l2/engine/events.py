#!filepath: src/l2/offline/events.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, List, Tuple, Optional

Side = Literal["B", "S"]


@dataclass
class TradeEvent:
    """
    原始逐笔成交事件（从 parquet / 实时feed 解析来的最底层事件）
    ts_ns: 纳秒级时间戳
    """
    ts_ns: int
    price: float
    qty: int
    security_id: str


@dataclass
class EnrichedTradeEvent(TradeEvent):
    """
    增强后的成交事件（带 burst/impact 等标签）
    """
    burst_id: int
    is_price_impact: bool


@dataclass
class OrderEvent:
    """
    逐笔委托事件（简化版）
    """
    ts_ns: int
    price: float
    qty: int
    side: Side
    security_id: str
    is_delete: bool = False


@dataclass
class OrderBookSnapshot:
    """
    订单簿快照（top-N 档）
    """
    ts_ns: int
    security_id: str
    bids: List[Tuple[float, int]]  # [(price, qty), ...]
    asks: List[Tuple[float, int]]
