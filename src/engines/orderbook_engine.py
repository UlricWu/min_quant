#!filepath: src/engines/orderbook_engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional, Dict, List


Side = Literal["B", "S"]
EventType = Literal["ADD", "CANCEL", "TRADE"]


@dataclass
class OrderBookEvent:
    """
    统一订单簿事件格式（Atomic Engine 内部使用，不依赖 pandas/arrow）。
    """
    ts_ns: int
    event: EventType
    side: Optional[Side]
    price: float
    volume: int


class OrderBookEngine:
    """
    纯逻辑订单簿引擎：
    - 维护 bids / asks（按价格聚合）
    - 处理 ADD / CANCEL / TRADE 事件
    - 支持生成 L1/Ln 盘口快照（levels）
    - 不做任何 I/O，不关心 symbol/date

    备注：
    - CANCEL 事件直接按 (side, price, volume) 减挂单（依赖供应商提供取消数量）
    - TRADE 事件按 side 消耗对手盘（近似模型，可后续替换为更精细逻辑）
    """

    def __init__(self) -> None:
        # price -> agg volume
        self.bids: Dict[float, int] = {}
        self.asks: Dict[float, int] = {}

    # ------------------------------------------------------------------
    def reset(self) -> None:
        self.bids.clear()
        self.asks.clear()

    # ------------------------------------------------------------------
    def on_event(self, evt: OrderBookEvent) -> None:
        if evt.event == "ADD":
            self._on_add(evt)
        elif evt.event == "CANCEL":
            self._on_cancel(evt)
        elif evt.event == "TRADE":
            self._on_trade(evt)

    # ------------------------------------------------------------------
    def snapshot(self, ts_ns: int, levels: int = 10) -> Dict[str, float | int | None]:
        """
        生成某一时刻的盘口快照（仅数据结构，不做 I/O）。

        返回字段示例：
            ts_ns
            best_bid, best_bid_size
            best_ask, best_ask_size
            spread
            bid_px_1, bid_sz_1, ..., bid_px_N, bid_sz_N
            ask_px_1, ask_sz_1, ..., ask_px_N, ask_sz_N
        """
        bid_prices = sorted(self.bids.keys(), reverse=True)
        ask_prices = sorted(self.asks.keys())

        best_bid = bid_prices[0] if bid_prices else None
        best_ask = ask_prices[0] if ask_prices else None

        if best_bid is not None and best_ask is not None:
            spread = best_ask - best_bid
        else:
            spread = None

        snap: Dict[str, float | int | None] = {
            "ts_ns": ts_ns,
            "best_bid": best_bid,
            "best_bid_size": self.bids.get(best_bid, 0) if best_bid is not None else 0,
            "best_ask": best_ask,
            "best_ask_size": self.asks.get(best_ask, 0) if best_ask is not None else 0,
            "spread": spread,
        }

        # 逐档写出
        for i in range(levels):
            # bids（从高到低）
            if i < len(bid_prices):
                px = bid_prices[i]
                sz = self.bids.get(px, 0)
            else:
                px = None
                sz = 0
            snap[f"bid_px_{i+1}"] = px
            snap[f"bid_sz_{i+1}"] = sz

            # asks（从低到高）
            if i < len(ask_prices):
                px = ask_prices[i]
                sz = self.asks.get(px, 0)
            else:
                px = None
                sz = 0
            snap[f"ask_px_{i+1}"] = px
            snap[f"ask_sz_{i+1}"] = sz

        return snap

    # ==================================================================
    # 内部处理逻辑
    # ==================================================================
    def _on_add(self, evt: OrderBookEvent) -> None:
        if evt.side not in ("B", "S"):
            return

        book = self.bids if evt.side == "B" else self.asks
        prev = book.get(evt.price, 0)
        book[evt.price] = prev + evt.volume

    def _on_cancel(self, evt: OrderBookEvent) -> None:
        """
        简化处理：供应商 cancel 事件通常含 Price + Volume，
        直接在 (side, price) 档位扣减 volume。
        """
        if evt.side not in ("B", "S"):
            return

        book = self.bids if evt.side == "B" else self.asks
        prev = book.get(evt.price, 0)
        new_vol = max(0, prev - evt.volume)
        if new_vol == 0:
            # 可以选择删 key，也可以保留为 0
            book.pop(evt.price, None)
        else:
            book[evt.price] = new_vol

    def _on_trade(self, evt: OrderBookEvent) -> None:
        """
        成交事件：按 side 消耗对手盘（近似逻辑）。

        - side='B' → 主动买，吃 asks
        - side='S' → 主动卖，吃 bids
        - side=None → 不调整（例如 SZ 无可靠 side）
        """
        if evt.side == "B":
            # 吃卖盘
            self._consume_book(self.asks, evt.price, evt.volume, ascending=True)
        elif evt.side == "S":
            # 吃买盘
            self._consume_book(self.bids, evt.price, evt.volume, ascending=False)
        else:
            # side 不可靠：先忽略，不调整订单簿
            return

    def _consume_book(
        self,
        book: Dict[float, int],
        price: float,
        volume: int,
        ascending: bool,
    ) -> None:
        """
        从一侧订单簿中消耗 volume。
        简化策略：优先从最优档开始吃，并在 price 附近吃。
        """
        if volume <= 0 or not book:
            return

        # 排序方向：asks 升序（ascending=True），bids 降序（ascending=False）
        prices = sorted(book.keys(), reverse=ascending)

        remaining = volume
        for px in prices:
            if ascending and px < price:
                # asks：只吃 >= price
                continue
            if not ascending and px > price:
                # bids：只吃 <= price
                continue

            avail = book.get(px, 0)
            if avail <= 0:
                continue

            take = min(avail, remaining)
            new_vol = avail - take
            if new_vol <= 0:
                book.pop(px, None)
            else:
                book[px] = new_vol

            remaining -= take
            if remaining <= 0:
                break
