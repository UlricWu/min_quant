#!filepath: src/l2/orderbook/order_event.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal, Optional
import pandas as pd


Side = Literal["B", "S"]  # 买/卖


@dataclass
class OrderAdd:
    order_id: int
    price: float
    volume: int
    side: Side
    ts: pd.Timestamp


@dataclass
class OrderCancel:
    order_id: int
    cancel_volume: Optional[int]  # 若为 None = 全撤
    ts: pd.Timestamp


@dataclass
class OrderTrade:
    order_id: int
    trade_volume: int
    trade_price: float
    ts: pd.Timestamp
