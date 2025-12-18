# src/engines/orderbook_rebuild_engine.py
from __future__ import annotations

from typing import Iterable, Iterator, Literal
from datetime import datetime
import heapq
from src.engines.context import EngineContext

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.engines.context import EngineContext
from src.l2.common.normalized_event import NormalizedEvent
from src import logs


#
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


@dataclass
class Order:
    order_id: int
    side: str  # B / S
    price: float
    volume: int
    ts: int


class OrderBook:
    """
    极简但正确的 OrderBook（重建用）

    设计目标：
    - 正确
    - 可回放
    - 可 snapshot
    """

    def __init__(self, symbol: str):
        self.symbol = symbol

        # order_id → Order
        self.orders: Dict[int, Order] = {}

        # price → [order_id, ...]（FIFO）
        self.bids: Dict[float, List[int]] = defaultdict(list)
        self.asks: Dict[float, List[int]] = defaultdict(list)

        self.last_ts: int | None = None

    # --------------------------------------------------
    # ADD
    # --------------------------------------------------
    def add_order(self, ev: NormalizedEvent) -> None:
        if ev.order_id in self.orders:
            # 交易所可能重复下发，直接忽略
            return

        if ev.side not in ("B", "S"):
            return

        order = Order(
            order_id=ev.order_id,
            side=ev.side,
            price=ev.price,
            volume=ev.volume,
            ts=ev.ts,
        )
        self.orders[ev.order_id] = order

        book = self.bids if ev.side == "B" else self.asks
        book[ev.price].append(ev.order_id)

        self.last_ts = ev.ts

    # --------------------------------------------------
    # CANCEL
    # --------------------------------------------------
    def cancel_order(self, ev: NormalizedEvent) -> None:
        order = self.orders.pop(ev.order_id, None)
        if order is None:
            return

        book = self.bids if order.side == "B" else self.asks
        ids = book.get(order.price)
        if ids:
            try:
                ids.remove(order.order_id)
            except ValueError:
                pass
            if not ids:
                book.pop(order.price, None)

        self.last_ts = ev.ts

    # --------------------------------------------------
    # TRADE
    # --------------------------------------------------
    def trade(self, ev: NormalizedEvent) -> None:
        """
        极简处理：
        - 用 order_id 减 volume
        - volume <= 0 → remove
        """
        order = self.orders.get(ev.order_id)
        if order is None:
            return

        order.volume -= ev.volume
        if order.volume <= 0:
            self.cancel_order(ev)

        self.last_ts = ev.ts

    # --------------------------------------------------
    # Snapshot
    # --------------------------------------------------
    def to_snapshot(self, depth: int = 10) -> pd.DataFrame:
        """
        输出标准盘口快照（L2 重建最小集）
        """
        rows = []

        # 买盘：价格从高到低
        for side, book, reverse in [
            ("B", self.bids, True),
            ("S", self.asks, False),
        ]:
            prices = sorted(book.keys(), reverse=reverse)[:depth]
            for lvl, price in enumerate(prices, start=1):
                qty = sum(self.orders[oid].volume for oid in book[price])
                rows.append(
                    {
                        "symbol": self.symbol,
                        "ts": self.last_ts,
                        "side": side,
                        "level": lvl,
                        "price": price,
                        "volume": qty,
                    }
                )

        return pd.DataFrame(rows)


class OrderBookRebuildEngine:
    """
    OrderBook 重建引擎（Offline + Realtime 共用）

    约束：
    - ts 必须是 int
    - 所有事件最终只走 _apply
    """

    def __init__(self):
        self.book: OrderBook | None = None

    # ======================================================
    # 唯一对外入口
    # ======================================================
    def execute(self, ctx: EngineContext) -> None:
        if self.book is None:
            self.book = OrderBook(symbol=ctx.symbol)

        if ctx.mode == "offline":
            assert ctx.input_path and ctx.output_path
            self._run_offline(ctx)
        else:
            assert ctx.event is not None
            self._apply(ctx.event)

            if ctx.emit_snapshot:
                self._emit_snapshot(ctx.output_path)

    # ======================================================
    # Offline 批处理
    # ======================================================
    def _run_offline(self, ctx: EngineContext) -> None:

        df = pd.read_parquet(ctx.input_path)

        for ev in self._iter_events(df):
            self._apply(ev)

        self._emit_snapshot(ctx.output_path)

    # ======================================================
    # 核心状态推进（唯一真相）
    # ======================================================
    def _apply(self, ev: NormalizedEvent) -> None:
        if ev.event == "ADD":
            self.book.add_order(ev)
        elif ev.event == "CANCEL":
            self.book.cancel_order(ev)
        elif ev.event == "TRADE":
            self.book.trade(ev)
        else:
            raise ValueError(f"Unknown event={ev.event}")

    # ======================================================
    # Snapshot
    # ======================================================
    def _emit_snapshot(self, out: Path | None) -> None:
        if out is None:
            return

        snapshot_df = self.book.to_snapshot()
        snapshot_df.to_parquet(out, index=False)

        logs.info(f"[OrderBook] snapshot written → {out}")

    # ======================================================
    # Offline Event Loader
    # ======================================================
    @staticmethod
    def _iter_events(df: pd.DataFrame) -> Iterable[NormalizedEvent]:
        for row in df.itertuples(index=False):
            yield NormalizedEvent.from_row(row)
