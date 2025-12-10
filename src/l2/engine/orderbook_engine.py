#!filepath: src/l2/offline/orderbook_engine.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple

from .events import OrderEvent, TradeEvent, OrderBookSnapshot


@dataclass
class OrderBookState:
    bids: Dict[float, int] = field(default_factory=dict)  # price → qty
    asks: Dict[float, int] = field(default_factory=dict)
    last_ts_ns: Optional[int] = None
    security_id: Optional[str] = None


class OrderBookEngine:
    """
    订单簿状态机（Streaming）
    - update_order / update_trade 维护内部状态
    - snapshot() 输出 top-N 快照
    - 不做 IO
    """

    def __init__(self, levels: int = 10) -> None:
        self.levels = levels
        self.state = OrderBookState()

    def reset(self) -> None:
        self.state = OrderBookState()

    # -------------------- 委托事件 --------------------
    def update_order(self, ev: OrderEvent) -> None:
        s = self.state
        s.last_ts_ns = ev.ts_ns
        s.security_id = ev.security_id

        book = s.bids if ev.side == "B" else s.asks

        if ev.is_delete:
            if ev.price in book:
                del book[ev.price]
        else:
            book[ev.price] = ev.qty

    # -------------------- 成交事件 --------------------
    def update_trade(self, ev: TradeEvent) -> None:
        """
        成交对 orderbook 的影响在真实市场中非常复杂。
        这里暂时只更新时间戳和标的，留出接口，之后可以结合挂单队列精细扣减。
        """
        s = self.state
        s.last_ts_ns = ev.ts_ns
        s.security_id = ev.security_id

    # -------------------- Snapshot --------------------
    def snapshot(self) -> Optional[OrderBookSnapshot]:
        s = self.state
        if s.last_ts_ns is None or s.security_id is None:
            return None

        bids_sorted: List[Tuple[float, int]] = sorted(
            s.bids.items(), key=lambda x: -x[0]
        )[: self.levels]
        asks_sorted: List[Tuple[float, int]] = sorted(
            s.asks.items(), key=lambda x: x[0]
        )[: self.levels]

        return OrderBookSnapshot(
            ts_ns=s.last_ts_ns,
            security_id=s.security_id,
            bids=bids_sorted,
            asks=asks_sorted,
        )
