#!filepath: src/l2/orderbook/orderbook_rebuilder.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.l2.orderbook.orderbook_store import OrderBook
from src.utils.datetime_utils import DateTimeUtils as dt
from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs


# =====================================================================
# Exchange Adapter: 统一 SH + SZ 四类表结构
# =====================================================================

class ExchangeAdapter:
    """
    将 SH/SZ 的 Order/Trade 不同结构统一为标准事件格式:

    Output unified fields:
        ts
        event = ADD / CANCEL / TRADE
        order_id   - 原始订单ID（SubSeq / BuyNo / SellNo）
        side       - B/S/None
        price
        volume
        buy_no     - 成交配对字段（如 SH Trade / SZ Trade）
        sell_no
    """

    # ------------------------------------------------------------------
    # 上交所：Order（A/D/T）
    # ------------------------------------------------------------------
    @staticmethod
    def parse_sh_order(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["ts"] = df["TradeTime"].map(dt.parse)

        df["event"] = df["TickType"].map({
            "A": "ADD",
            "D": "CANCEL",
            "T": "TRADE",
        }).fillna('E')

        df["order_id"] = df["SubSeq"].astype(int)
        df["side"] = df["Side"].map({1: "B", 2: "S"})
        df["price"] = df["Price"].astype(float)
        df["volume"] = df["Volume"].astype(int)
        df["buy_no"] = df["BuyNo"].astype(int)
        df["sell_no"] = df["SellNo"].astype(int)

        return df[["ts", "event", "order_id", "side",
                   "price", "volume", "buy_no", "sell_no"]]

    # ------------------------------------------------------------------
    # 上交所：Trade（TickType = T）
    # ------------------------------------------------------------------
    @staticmethod
    def parse_sh_trade(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df = df[df["TickType"] == "T"]

        df["ts"] = df["TradeTime"].map(dt.parse)
        df["event"] = "TRADE"
        df["order_id"] = df["SubSeq"].astype(int)
        df["side"] = df["Side"].map({1: "B", 2: "S"})
        df["price"] = df["Price"].astype(float)
        df["volume"] = df["Volume"].astype(int)
        df["buy_no"] = df["BuyNo"].astype(int)
        df["sell_no"] = df["SellNo"].astype(int)

        return df[["ts", "event", "order_id", "side",
                   "price", "volume", "buy_no", "sell_no"]]

    # ------------------------------------------------------------------
    # 深圳：Order（无 TickType，依靠 OrderType）
    # ------------------------------------------------------------------
    @staticmethod
    def parse_sz_order(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["ts"] = df["TradeTime"].map(dt.parse)

        def order_event(x):
            if x == 0:
                return "CANCEL"
            if x in (1, 2, 3):
                return "ADD"
            return None

        df["event"] = df["OrderType"].map(order_event)

        df["order_id"] = df["SubSeq"].astype(int)
        df["side"] = df["Side"].map({1: "B", 2: "S"})
        df["price"] = df["Price"].astype(float)
        df["volume"] = df["Volume"].astype(int)
        df["buy_no"] = 0
        df["sell_no"] = 0

        return df[["ts", "event", "order_id", "side",
                   "price", "volume", "buy_no", "sell_no"]]

    # ------------------------------------------------------------------
    # 深圳：Trade（ExecType = 1 成交，2 撤单确认）
    # ------------------------------------------------------------------
    @staticmethod
    def parse_sz_trade(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["ts"] = df["TradeTime"].map(dt.parse)

        def trade_event(x):
            if x == 1:
                return "TRADE"
            if x == 2:
                return "CANCEL"
            return None

        df["event"] = df["ExecType"].map(trade_event)

        df["order_id"] = df["SubSeq"].astype(int)
        df["price"] = df["TradePrice"].astype(float)
        df["volume"] = df["TradeVolume"].astype(int)
        df["buy_no"] = df["BuyNo"].astype(int)
        df["sell_no"] = df["SellNo"].astype(int)
        df["side"] = None

        return df[["ts", "event", "order_id", "side",
                   "price", "volume", "buy_no", "sell_no"]]


# =====================================================================
# Main OrderBook Rebuilder
# =====================================================================

@dataclass
class OrderBookRebuilder:
    """
    从 SH/SZ 官方逐笔数据重建订单簿 + L1-LN 快照。
    自动识别交易所并使用对应适配器。
    """
    book_levels: int = 10
    path: PathManager = PathManager()

    # ------------------------------------------------------------------
    def build(self, symbol: str, date: str, write: bool = False) -> Optional[pd.DataFrame]:
        snapshot_path = self.path.snapshot_dir(symbol, date)

        if snapshot_path.exists():
            logs.debug(f"[Snapshot] {symbol} 已存在 → 跳过: {snapshot_path}")
            return

        # 读取 Order/Trade
        order_path = self.path.order_dir(symbol, date)
        trade_path = self.path.trade_dir(symbol, date)

        if not order_path.exists():
            raise FileNotFoundError(f"Order parquet not found: {order_path}")
        if not trade_path.exists():
            raise FileNotFoundError(f"Trade parquet not found: {trade_path}")


        df_order = pq.read_table(order_path).to_pandas()
        df_trade = pq.read_table(trade_path).to_pandas()

        # 自动识别交易所
        exchange = int(df_order["ExchangeID"].iloc[1])
        logs.info(f"[OrderBook] 交易所类型 ExchangeID={exchange} (1=SH, 2=SZ)")

        if exchange == 1:  # 上海
            df1 = ExchangeAdapter.parse_sh_order(df_order)
            df2 = ExchangeAdapter.parse_sh_trade(df_trade)
        elif exchange == 2:  # 深圳
            df1 = ExchangeAdapter.parse_sz_order(df_order)
            df2 = ExchangeAdapter.parse_sz_trade(df_trade)
        else:
            raise ValueError(f"未知交易所 ExchangeID={exchange}")

        # 合并事件流
        events = (
            pd.concat([df1, df2], ignore_index=True)
            .dropna(subset=["event"])
            .sort_values("ts")
            .reset_index(drop=True)
        )

        # 创建订单簿
        book = OrderBook()
        order_map: Dict[int, Dict] = {}
        snapshots = []

        # ---------------------------------------------------------
        # Replay 所有事件
        # ---------------------------------------------------------
        for _, row in events.iterrows():
            ts = row["ts"]

            # ========= 过滤集合竞价 =========
            if ts.hour < 9 or (ts.hour == 9 and ts.minute < 30):
                continue

            ev = row["event"]
            oid = row["order_id"]

            # ------------------------------------------------------------------
            # 新增委托
            # ------------------------------------------------------------------
            if ev == "ADD":
                side = row["side"]
                price = float(row["price"])
                vol = int(row["volume"])

                order_map[oid] = {"price": price, "side": side, "remaining": vol}
                book.add_order(side, price, vol)

            # ------------------------------------------------------------------
            # 撤单
            # ------------------------------------------------------------------
            elif ev == "CANCEL":
                if oid not in order_map:
                    continue
                info = order_map[oid]

                cancel_vol = min(int(row["volume"]), info["remaining"])
                info["remaining"] -= cancel_vol
                if info["remaining"] <= 0:
                    del order_map[oid]

                book.cancel_order(info["side"], info["price"], cancel_vol)

            # ------------------------------------------------------------------
            # 成交（支持 SH/SZ BuyNo/SellNo）
            # ------------------------------------------------------------------
            elif ev == "TRADE":
                trade_vol = int(row["volume"])

                buy_no = int(row["buy_no"]) if row["buy_no"] != 0 else None
                sell_no = int(row["sell_no"]) if row["sell_no"] != 0 else None

                # 买方订单
                if buy_no is not None and buy_no in order_map:
                    info = order_map[buy_no]
                    vol = min(trade_vol, info["remaining"])
                    info["remaining"] -= vol
                    if info["remaining"] <= 0:
                        del order_map[buy_no]
                    book.trade("B", info["price"], vol)

                # 卖方订单
                if sell_no is not None and sell_no in order_map:
                    info = order_map[sell_no]
                    vol = min(trade_vol, info["remaining"])
                    info["remaining"] -= vol
                    if info["remaining"] <= 0:
                        del order_map[sell_no]
                    book.trade("S", info["price"], vol)

            # ------------------------------------------------------------------
            # 输出快照
            # ------------------------------------------------------------------
            snap = book.get_snapshot()
            snapshots.append(self._format_snapshot(ts, snap))

        if not snapshots:
            logs.warning(f"[OrderBook] 无有效连续竞价事件: {symbol} {date}")
            return None

        snap_df = (
            pd.DataFrame(snapshots)
            .sort_values("ts")
            .reset_index(drop=True)
        )

        logs.info(
            f"[OrderBook] 完成重建: {symbol} {date}, "
            f"events={len(events)}, snapshots={len(snap_df)}"
        )

        if write:
            self.write(symbol, date, snap_df)

        return snap_df

    # ------------------------------------------------------------------
    # 写入 Snapshot.parquet
    # ------------------------------------------------------------------
    def write(self, symbol: str, date: str, df: pd.DataFrame):
        out = self.path.snapshot_dir(symbol, date)
        FileSystem.ensure_dir(out.parent)

        logs.info(f"[SnapshotWriter] 写入 Snapshot: {out}")
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, out, compression="zstd")

    # ------------------------------------------------------------------
    # 将深度展开为 L1-LN 行
    # ------------------------------------------------------------------

    def _format_snapshot(self, ts, snap: dict, levels=None):
        if not levels:
            levels = self.book_levels
        row = {"ts": ts}

        bids = snap["bid_prices"]
        bid_vol = snap["bid_volumes"]
        asks = snap["ask_prices"]
        ask_vol = snap["ask_volumes"]

        for i in range(levels):
            row[f"BidPrice{i + 1}"] = bids[i] if i < len(bids) else None
            row[f"BidVolume{i + 1}"] = bid_vol[i] if i < len(bid_vol) else 0
            row[f"AskPrice{i + 1}"] = asks[i] if i < len(asks) else None
            row[f"AskVolume{i + 1}"] = ask_vol[i] if i < len(ask_vol) else 0

        return row
