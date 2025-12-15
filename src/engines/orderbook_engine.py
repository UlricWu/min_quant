from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Literal
from datetime import datetime
import heapq


# ===============================
# InternalEvent（唯一输入事实）
# ===============================
@dataclass(frozen=True)
class InternalEvent:
    ts: datetime
    event: Literal["ADD", "CANCEL", "TRADE"]
    order_id: int
    side: Literal["B", "S"] | None
    price: float
    volume: int
    buy_no: int
    sell_no: int


# ===============================
# Output
# ===============================
@dataclass
class OrderBookSnapshot:
    ts: datetime
    bids: list[tuple[float, int]]
    asks: list[tuple[float, int]]


# ===============================
# 内部 OrderBook（极简可用版）
# ===============================
class OrderBook:
    def __init__(self):
        self.bids: dict[float, int] = {}
        self.asks: dict[float, int] = {}

    def apply(self, ev: InternalEvent) -> None:
        if ev.event == "ADD":
            book = self.bids if ev.side == "B" else self.asks
            book[ev.price] = book.get(ev.price, 0) + ev.volume

        elif ev.event == "CANCEL":
            book = self.bids if ev.side == "B" else self.asks
            if ev.price in book:
                book[ev.price] -= ev.volume
                if book[ev.price] <= 0:
                    del book[ev.price]

        elif ev.event == "TRADE":
            # 成交默认从对手盘减
            if ev.side == "B":
                book = self.asks
            elif ev.side == "S":
                book = self.bids
            else:
                return

            if ev.price in book:
                book[ev.price] -= ev.volume
                if book[ev.price] <= 0:
                    del book[ev.price]

    def snapshot(self, ts: datetime) -> OrderBookSnapshot:
        bids = sorted(self.bids.items(), key=lambda x: -x[0])
        asks = sorted(self.asks.items(), key=lambda x: x[0])
        return OrderBookSnapshot(ts=ts, bids=bids, asks=asks)


# ===============================
# Engine（最终接口）
# ===============================
class OrderBookRebuildEngine:
    """
    纯 OrderBook 重建引擎：
    - 不读文件
    - 不识别 SH/SZ
    - 不关心 symbol
    """

    def rebuild(
        self,
        events: Iterable[InternalEvent],
        *,
        snapshot_interval_ms: int | None = None,
    ) -> Iterator[OrderBookSnapshot]:

        book = OrderBook()
        last_ts: datetime | None = None

        for ev in events:
            book.apply(ev)

            if snapshot_interval_ms is None:
                yield book.snapshot(ev.ts)
            else:
                if last_ts is None:
                    last_ts = ev.ts
                    yield book.snapshot(ev.ts)
                else:
                    delta_ms = (ev.ts - last_ts).total_seconds() * 1000
                    if delta_ms >= snapshot_interval_ms:
                        last_ts = ev.ts
                        yield book.snapshot(ev.ts)
