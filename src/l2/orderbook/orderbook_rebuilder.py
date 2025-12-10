#!filepath: src/l2/orderbook/orderbook_rebuilder.py
from __future__ import annotations

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from dataclasses import dataclass
from typing import Dict

from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs

from src.l2.common.event_parser import parse_events


# ============================================================
# 简易订单簿维护结构
# ============================================================
class OrderBook:
    """
    使用价格 → 剩余量 的结构维护盘口。
    """

    def __init__(self):
        self.bid: Dict[float, int] = {}  # 买盘：价格降序
        self.ask: Dict[float, int] = {}  # 卖盘：价格升序

    # -------------------------
    # ADD
    # -------------------------
    def add_order(self, side: str, price: float, volume: int):
        book = self.bid if side == "B" else self.ask
        book[price] = book.get(price, 0) + volume

    # -------------------------
    # CANCEL
    # -------------------------
    def cancel_order(self, side: str, price: float, vol: int):
        book = self.bid if side == "B" else self.ask
        if price in book:
            remaining = max(book[price] - vol, 0)
            if remaining == 0:
                del book[price]
            else:
                book[price] = remaining

    # -------------------------
    # TRADE
    # -------------------------
    def trade(self, side: str, price: float, vol: int):
        """
        side: B 表示来自买方主动单 → 冲击 ask
        side: S 表示卖方主动单 → 冲击 bid
        """
        if side == "B":  # 买主动 → 卖盘吃掉
            book = self.ask
        else:  # 卖主动 → 买盘吃掉
            book = self.bid

        if price in book:
            new_vol = max(book[price] - vol, 0)
            if new_vol == 0:
                del book[price]
            else:
                book[price] = new_vol

    # -------------------------
    # 获取 L1–Ln
    # -------------------------
    def get_snapshot(self, levels: int = 10):
        bids_sorted = sorted(self.bid.items(), key=lambda x: -x[0])
        asks_sorted = sorted(self.ask.items(), key=lambda x: x[0])

        bid_prices = [p for p, v in bids_sorted][:levels]
        bid_volumes = [v for p, v in bids_sorted][:levels]
        ask_prices = [p for p, v in asks_sorted][:levels]
        ask_volumes = [v for p, v in asks_sorted][:levels]

        return {
            "bid_prices": bid_prices,
            "bid_volumes": bid_volumes,
            "ask_prices": ask_prices,
            "ask_volumes": ask_volumes,
        }


# ============================================================
# 主类：OrderBookRebuilder
# ============================================================
@dataclass
class OrderBookRebuilder:
    book_levels: int = 10
    path: PathManager = PathManager()

    # ---------------------------------------------------------
    def build(self, symbol: str, date: str, write: bool = False):
        snapshot_path = self.path.snapshot_dir(symbol, date)
        if snapshot_path.exists():
            logs.info(f"[Snapshot] 已存在 → 跳过: {snapshot_path}")
            return

        # 读取数据
        order_path = self.path.order_dir(symbol, date)
        trade_path = self.path.trade_dir(symbol, date)

        # if not order_path.exists():
        #     return
        #     # raise FileNotFoundError(order_path)
        # if not trade_path.exists():
        #     # raise FileNotFoundError(trade_path)

        df_order = pq.read_table(order_path).to_pandas()
        df_trade = pq.read_table(trade_path).to_pandas()

        exchange_id = int(df_order["ExchangeID"].iloc[0])
        logs.info(f"[OrderBook] Exchange = {exchange_id} (1=SH, 2=SZ)")

        # 解析事件
        ev_order = parse_events(df_order, "order")
        ev_trade = parse_events(df_trade, "trade")

        events = (
            pd.concat([ev_order, ev_trade], ignore_index=True)
            .sort_values("ts")
            .reset_index(drop=True)
        )

        book = OrderBook()
        order_map: Dict[int, Dict] = {}
        snapshots = []

        # 回放所有事件
        for _, e in events.iterrows():
            ts = e["ts"]

            # ==========================
            # 集合竞价过滤 (<9:30)
            # ==========================
            if ts.hour < 9 or (ts.hour == 9 and ts.minute < 30):
                continue

            ev = e["event"]
            oid = int(e["order_id"])
            side = e["side"]
            price = float(e["price"])
            vol = int(e["volume"])
            buy_no = int(e["buy_no"])
            sell_no = int(e["sell_no"])

            # -------------------------
            # ADD
            # -------------------------
            if ev == "ADD":
                order_map[oid] = {"price": price, "side": side, "remaining": vol}
                book.add_order(side, price, vol)

            # -------------------------
            # CANCEL
            # -------------------------
            elif ev == "CANCEL":
                if oid in order_map:
                    info = order_map[oid]
                    cancel_vol = min(vol, info["remaining"])
                    info["remaining"] -= cancel_vol
                    if info["remaining"] <= 0:
                        del order_map[oid]
                    book.cancel_order(info["side"], info["price"], cancel_vol)

            # -------------------------
            # TRADE
            # -------------------------
            elif ev == "TRADE":
                # 买方主动
                if buy_no in order_map:
                    info = order_map[buy_no]
                    eat_vol = min(vol, info["remaining"])
                    info["remaining"] -= eat_vol
                    if info["remaining"] <= 0:
                        del order_map[buy_no]
                    book.trade("B", info["price"], eat_vol)

                # 卖方主动
                if sell_no in order_map:
                    info = order_map[sell_no]
                    eat_vol = min(vol, info["remaining"])
                    info["remaining"] -= eat_vol
                    if info["remaining"] <= 0:
                        del order_map[sell_no]
                    book.trade("S", info["price"], eat_vol)

            # 记录快照
            snapshots.append(self.format_snapshot(ts, book.get_snapshot(self.book_levels)))

        if not snapshots:
            logs.warning(f"[OrderBookRebuilder] 无连续竞价事件 {symbol}-{date}")
            return None

        df = pd.DataFrame(snapshots).sort_values("ts").reset_index(drop=True)

        logs.info(
            f"[OrderBook] 完成重建: symbol={symbol}, date={date}, "
            f"events={len(events)}, snapshots={len(df)}"
        )

        if write:
            self.write(symbol, date, df)

    # ---------------------------------------------------------
    def format_snapshot(self, ts, snap):
        row = {"ts": ts}
        for i in range(self.book_levels):
            row[f"BidPrice{i + 1}"] = snap["bid_prices"][i] if i < len(snap["bid_prices"]) else None
            row[f"BidVolume{i + 1}"] = snap["bid_volumes"][i] if i < len(snap["bid_volumes"]) else 0
            row[f"AskPrice{i + 1}"] = snap["ask_prices"][i] if i < len(snap["ask_prices"]) else None
            row[f"AskVolume{i + 1}"] = snap["ask_volumes"][i] if i < len(snap["ask_volumes"]) else 0
        return row

    # ---------------------------------------------------------
    def write(self, symbol, date, df):
        path = self.path.snapshot_dir(symbol, date)
        FileSystem.ensure_dir(path.parent)
        pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path, compression="zstd")
