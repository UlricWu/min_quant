#!filepath: src/adapters/orderbook_adapter.py
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import List

import pandas as pd

from src.engines.orderbook_engine import OrderBookEngine, OrderBookEvent
from src.l2.common.event_parser import parse_events
from src.utils.filesystem import FileSystem
from src import logs


class OrderBookAdapter:
    """
    I/O Adapter：
    - 负责从 symbol 级 Order / Trade parquet 加载数据
    - 调用 parse_events() 统一事件结构
    - 喂给 OrderBookEngine
    - 收集 snapshot 写出 Snapshot.parquet

    注意：
    - 只负责 I/O 和 orchestrate，不写微观结构逻辑。
    """

    def __init__(self, path_manager, levels: int = 10):
        self.pm = path_manager
        self.levels = levels

    # ------------------------------------------------------------------
    def build_symbol_orderbook(
        self,
        symbol: int,
        date: str,
        engine: OrderBookEngine,
    ) -> None:
        """
        为单个 symbol+date 生成 Snapshot.parquet
        路径：
            data/symbol/<symbol>/<date>/Order.parquet
            data/symbol/<symbol>/<date>/Trade.parquet
            → Snapshot.parquet
        """
        sym_dir = self.pm.symbol_dir(symbol, date)
        order_path = sym_dir / "Order.parquet"
        trade_path = sym_dir / "Trade.parquet"
        snapshot_path = sym_dir / "Snapshot.parquet"

        if not order_path.exists():
            logs.warning(f"[OrderBookAdapter] {order_path} 不存在，跳过 symbol={symbol}")
            return
        if not trade_path.exists():
            logs.warning(f"[OrderBookAdapter] {trade_path} 不存在，跳过 symbol={symbol}")
            return

        # 如果 Snapshot 已存在，可以视需求决定是否跳过
        if snapshot_path.exists():
            logs.info(
                f"[OrderBookAdapter] Snapshot 已存在 → skip symbol={symbol}, date={date}"
            )
            return

        logs.info(
            f"[OrderBookAdapter] 重建 OrderBook snapshot: symbol={symbol}, date={date}"
        )

        # --------------------------------------------------------------
        # 1) 读取原始逐笔（symbol 级别，体量已比较小）
        # --------------------------------------------------------------
        df_order = pd.read_parquet(order_path)
        df_trade = pd.read_parquet(trade_path)

        # --------------------------------------------------------------
        # 2) 统一解析结构：InternalEvent（使用你现有 parse_events）
        # --------------------------------------------------------------
        ev_order = parse_events(df_order, kind="order")
        ev_trade = parse_events(df_trade, kind="trade")

        # ev_* 应至少包含：
        #   ts (datetime64[ns, Asia/Shanghai])
        #   event (ADD/CANCEL/TRADE)
        #   side ('B'/'S'/None)
        #   price (float)
        #   volume (int)
        # 这里增加 ts_ns 方便排序和 snapshot 对齐
        ev_order = ev_order.copy()
        ev_trade = ev_trade.copy()
        ev_order["ts_ns"] = ev_order["ts"].view("int64")
        ev_trade["ts_ns"] = ev_trade["ts"].view("int64")

        all_ev = pd.concat([ev_order, ev_trade], ignore_index=True)
        all_ev.sort_values("ts_ns", inplace=True)
        all_ev.reset_index(drop=True, inplace=True)

        # --------------------------------------------------------------
        # 3) 逐事件推进 Engine，并在每个事件后生成 snapshot
        # --------------------------------------------------------------
        engine.reset()
        snapshots: List[dict] = []

        for row in all_ev.itertuples(index=False):
            evt = OrderBookEvent(
                ts_ns=int(row.ts_ns),
                event=row.event,
                side=row.side if row.side in ("B", "S") else None,
                price=float(row.price),
                volume=int(row.volume),
            )
            engine.on_event(evt)

            snap = engine.snapshot(evt.ts_ns, levels=self.levels)
            snapshots.append(snap)

        if not snapshots:
            logs.warning(
                f"[OrderBookAdapter] 没有生成任何 snapshot: symbol={symbol}, date={date}"
            )
            return

        # --------------------------------------------------------------
        # 4) 写出 Snapshot.parquet
        # --------------------------------------------------------------
        FileSystem.ensure_dir(sym_dir)
        df_snap = pd.DataFrame(snapshots)
        df_snap.to_parquet(snapshot_path, index=False)

        logs.info(
            f"[OrderBookAdapter] 写出 Snapshot: {snapshot_path}, rows={len(df_snap)}"
        )
