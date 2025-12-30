#!filepath: src/engines/orderbook_rebuild_engine.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Deque, Literal, Optional
from collections import defaultdict, deque

import pyarrow as pa
import pyarrow.parquet as pq

from src.pipeline.context import EngineContext
from src import logs


# ===============================
# Order / Book
# ===============================
@dataclass(slots=True)
class Order:
    order_id: int
    side: Literal["B", "S"]
    price: float
    volume: int
    ts: int  #  必须是 int


class OrderBook:
    """
    极简但正确的 OrderBook（重建用）— Arrow-friendly / 快版本

    关键优化（不改语义）：
    - bids/asks: price -> deque(order_id) 维持 FIFO
    - CANCEL 不再 ids.remove(order_id)（O(n)）
      改为：orders.pop(order_id) + level_volume 扣减 + lazy deletion
    - 每个价位维护 level_volume，snapshot 不再 sum(orders[oid].volume ...)
    """

    def __init__(self) -> None:
        # order_id -> Order
        self.orders: Dict[int, Order] = {}

        # price -> FIFO order ids (may contain stale ids; lazy cleaned)
        self.bids: Dict[float, Deque[int]] = defaultdict(deque)
        self.asks: Dict[float, Deque[int]] = defaultdict(deque)

        # price -> aggregated volume on that level
        self.bid_qty: Dict[float, int] = defaultdict(int)
        self.ask_qty: Dict[float, int] = defaultdict(int)

        self.last_ts: Optional[int] = None

    # --------------------------------------------------
    def add_order(self, *, ts: int, order_id: int, side: Optional[str], price: Optional[float],
                  volume: Optional[int]) -> None:
        if side not in ("B", "S") or price is None or volume is None:
            return
        if order_id in self.orders:
            # 交易所可能重复下发，忽略
            self.last_ts = ts
            return

        o = Order(order_id=order_id, side=side, price=float(price), volume=int(volume), ts=int(ts))
        self.orders[order_id] = o

        if side == "B":
            self.bids[o.price].append(order_id)
            self.bid_qty[o.price] += o.volume
        else:
            self.asks[o.price].append(order_id)
            self.ask_qty[o.price] += o.volume

        self.last_ts = ts

    # --------------------------------------------------
    def cancel_order(self, *, ts: int, order_id: int) -> None:
        o = self.orders.pop(order_id, None)
        if o is None:
            self.last_ts = ts
            return

        if o.side == "B":
            self.bid_qty[o.price] -= o.volume
            if self.bid_qty[o.price] <= 0:
                self.bid_qty.pop(o.price, None)
                # bids[o.price] deque 里可能还有 stale ids，无需立刻清理
                self.bids.pop(o.price, None)
        else:
            self.ask_qty[o.price] -= o.volume
            if self.ask_qty[o.price] <= 0:
                self.ask_qty.pop(o.price, None)
                self.asks.pop(o.price, None)

        self.last_ts = ts

    # --------------------------------------------------
    def trade(self, *, ts: int, order_id: int, volume: Optional[int]) -> None:
        """
        仍然按你原语义：用 order_id 减 volume，<=0 视为撤单
        """
        if volume is None:
            self.last_ts = ts
            return

        o = self.orders.get(order_id)
        if o is None:
            self.last_ts = ts
            return

        dv = int(volume)
        if dv <= 0:
            self.last_ts = ts
            return

        # 扣减聚合量
        if o.side == "B":
            self.bid_qty[o.price] -= dv
        else:
            self.ask_qty[o.price] -= dv

        o.volume -= dv

        if o.volume <= 0:
            # cancel_order 会再扣一次 o.volume（已变负）会出错，所以这里走专用清理
            self._remove_filled(ts=ts, o=o)
        else:
            # 仍有剩余，保证聚合量不为负（保护）
            if o.side == "B" and self.bid_qty[o.price] < 0:
                self.bid_qty[o.price] = 0
            if o.side == "S" and self.ask_qty[o.price] < 0:
                self.ask_qty[o.price] = 0
            self.last_ts = ts

    def _remove_filled(self, *, ts: int, o: Order) -> None:
        # o 已经在 orders 里
        self.orders.pop(o.order_id, None)

        # 价位聚合量如果被扣到 <=0，直接移除价位
        if o.side == "B":
            if self.bid_qty.get(o.price, 0) <= 0:
                self.bid_qty.pop(o.price, None)
                self.bids.pop(o.price, None)
        else:
            if self.ask_qty.get(o.price, 0) <= 0:
                self.ask_qty.pop(o.price, None)
                self.asks.pop(o.price, None)

        self.last_ts = ts

    # --------------------------------------------------
    def snapshot_table(self, depth: int = 10) -> pa.Table:
        """
        输出 L2 快照（最小集）：
          ts, side, level, price, volume

        注意：volume 直接来自 bid_qty/ask_qty（O(depth log P)）
        """
        ts = self.last_ts if self.last_ts is not None else 0

        rows_ts: list[int] = []
        rows_side: list[str] = []
        rows_level: list[int] = []
        rows_price: list[float] = []
        rows_vol: list[int] = []

        # 买盘：高到低
        bid_prices = sorted(self.bid_qty.keys(), reverse=True)[:depth]
        for lvl, p in enumerate(bid_prices, start=1):
            q = int(self.bid_qty[p])
            rows_ts.append(ts)
            rows_side.append("B")
            rows_level.append(lvl)
            rows_price.append(float(p))
            rows_vol.append(q)

        # 卖盘：低到高
        ask_prices = sorted(self.ask_qty.keys(), reverse=False)[:depth]
        for lvl, p in enumerate(ask_prices, start=1):
            q = int(self.ask_qty[p])
            rows_ts.append(ts)
            rows_side.append("S")
            rows_level.append(lvl)
            rows_price.append(float(p))
            rows_vol.append(q)

        schema = pa.schema(
            [
                ("ts", pa.int64()),
                ("side", pa.string()),
                ("level", pa.int16()),
                ("price", pa.float64()),
                ("volume", pa.int64()),
            ]
        )

        return pa.table(
            {
                "ts": pa.array(rows_ts, type=pa.int64()),
                "side": pa.array(rows_side, type=pa.string()),
                "level": pa.array(rows_level, type=pa.int16()),
                "price": pa.array(rows_price, type=pa.float64()),
                "volume": pa.array(rows_vol, type=pa.int64()),
            },
            schema=schema,
        )


# ===============================
# Engine
# ===============================
class OrderBookRebuildEngine:
    """
    OrderBook 重建引擎（Offline + Realtime 共用）
    - Arrow-only IO
    - 不再走 pandas / itertuples / NormalizedEvent.from_row
    - 仍然保持你的唯一真相：所有事件最终只走 _apply

    产物：
      - orderbook.parquet         (snapshot)
      - orderbook_events.parquet  (event stream)
    """
    EVENT_FLUSH_SIZE = 100_000

    def __init__(self, record_events: bool = False) -> None:
        self.record_events = record_events
        self.book: Optional[OrderBook] = None
        # event buffers (columnar)
        self._ev_ts: list[int] = []
        self._ev_event: list[str] = []
        self._ev_order_id: list[int] = []
        self._ev_side: list[Optional[str]] = []
        self._ev_price: list[Optional[float]] = []
        self._ev_volume: list[int] = []
        self._ev_notional: list[float] = []

        self._event_writer: Optional[pq.ParquetWriter] = None

    # ======================================================
    def execute(self, ctx: EngineContext) -> None:
        if self.book is None:
            self.book = OrderBook()

        if ctx.mode == "offline":
            assert ctx.input_file and ctx.output_file
            self._run_offline(ctx.input_file, ctx.output_file)
            return
        # realtime
        assert ctx.event is not None
        ev = ctx.event
        self._apply(
            ts=int(ev.ts),
            event=str(ev.event),
            order_id=int(ev.order_id),
            side=ev.side,
            price=float(ev.price) if ev.price is not None else None,
            volume=int(ev.volume) if ev.volume is not None else None,
        )
        if ctx.emit_snapshot:
            assert ctx.output_path is not None
            self._emit_snapshot(ctx.output_path)

    # ======================================================
    def _run_offline(self, input_path: Path, output_path: Path) -> None:
        pf = pq.ParquetFile(input_path)

        # record_events=True 才生成 orderbook_events.parquet
        if self.record_events:
            event_out = output_path.with_name("orderbook_events.parquet")
            event_schema = pa.schema(
                [
                    ("ts", pa.int64()),
                    ("event", pa.string()),
                    ("order_id", pa.int64()),
                    ("side", pa.string()),
                    ("price", pa.float64()),
                    ("volume", pa.int64()),
                    ("notional", pa.float64()),
                ]
            )
            self._event_writer = pq.ParquetWriter(event_out, event_schema)
        else:
            self._event_writer = None

        # 只读重建需要的列（真裁剪）
        cols = ["ts", "event", "order_id", "side", "price", "volume"]

        for batch in pf.iter_batches(columns=cols):

            # ✅ 关键：一次性转 pylist，避免 per-row as_py()
            ts_list = batch.column(0).to_pylist()
            ev_list = batch.column(1).to_pylist()
            oid_list = batch.column(2).to_pylist()
            side_list = batch.column(3).to_pylist()
            price_list = batch.column(4).to_pylist()
            vol_list = batch.column(5).to_pylist()

            n = batch.num_rows
            # 必须逐事件推进状态（orderbook 的本质），但避免构造对象

            for i in range(n):
                self._apply(
                    ts=int(ts_list[i]),
                    event=ev_list[i],
                    order_id=int(oid_list[i]),
                    side=side_list[i],
                    price=price_list[i],
                    volume=vol_list[i],
                )

        if self.record_events:
            self._flush_events()
            assert self._event_writer is not None
            self._event_writer.close()
            self._event_writer = None

        self._emit_snapshot(output_path)

    # ======================================================
    def _apply(
            self,
            *,
            ts: int,
            event: str,
            order_id: int,
            side: Optional[str],
            price: Optional[float],
            volume: Optional[int],
    ) -> None:
        assert self.book is not None

        if event == "ADD":
            self.book.add_order(ts=ts, order_id=order_id, side=side, price=price, volume=volume)
        elif event == "CANCEL":
            self.book.cancel_order(ts=ts, order_id=order_id)
        elif event == "TRADE":
            self.book.trade(ts=ts, order_id=order_id, volume=volume)
        else:
            raise ValueError(f"Unknown event={event}")

            # -------------------------------
            # ② 再记录规范化事件（新增）
            # -------------------------------
        if self.record_events:
            v = int(volume) if volume is not None else 0
            p = float(price) if price is not None else 0.0

            self._ev_ts.append(ts)
            self._ev_event.append(event)
            self._ev_order_id.append(order_id)
            self._ev_side.append(side)
            self._ev_price.append(p if price is not None else None)
            self._ev_volume.append(v)
            self._ev_notional.append(p * v if price is not None else 0.0)
            if len(self._ev_ts) >= self.EVENT_FLUSH_SIZE:
                self._flush_events()

        # ======================================================

    def _flush_events(self) -> None:
        if not self.record_events:
            return
        if not self._ev_ts:
            return
        assert self._event_writer is not None

        batch = pa.record_batch(
            [
                pa.array(self._ev_ts, pa.int64()),
                pa.array(self._ev_event, pa.string()),
                pa.array(self._ev_order_id, pa.int64()),
                pa.array(self._ev_side, pa.string()),
                pa.array(self._ev_price, pa.float64()),
                pa.array(self._ev_volume, pa.int64()),
                pa.array(self._ev_notional, pa.float64()),
            ],
            names=[
                "ts",
                "event",
                "order_id",
                "side",
                "price",
                "volume",
                "notional",
            ],
        )

        self._event_writer.write_batch(batch)

        self._ev_ts.clear()
        self._ev_event.clear()
        self._ev_order_id.clear()
        self._ev_side.clear()
        self._ev_price.clear()
        self._ev_volume.clear()
        self._ev_notional.clear()

    # ======================================================
    def _emit_snapshot(self, out: Path) -> None:
        assert self.book is not None
        table = self.book.snapshot_table(depth=10)
        pq.write_table(table, out)
