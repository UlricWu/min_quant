# src/l2/common/event_parser.py
from __future__ import annotations

from typing import Literal
import pandas as pd

from src import logs

EventKind = Literal["order", "trade"]

# ============================
# 内部统一输出 schema（顺序固定）
# ============================
OUT_COLS = [
    "symbol",
    "ts",
    "event",
    "order_id",
    "side",
    "price",
    "volume",
    "buy_no",
    "sell_no",
]


# ============================
# ts 向量化工具
# ============================
def build_ts_from_tick(
        *,
        date_str: str,
        tick: pd.Series,
) -> pd.Series:
    """
    date_str: 'YYYY-MM-DD'
    tick: HHMMSSmmm (int)
    return: int64 微秒时间戳
    """
    base_ts = int(
        pd.Timestamp(date_str, tz="Asia/Shanghai").timestamp() * 1_000_000
    )

    tick = tick.astype("int64")

    hh = tick // 10_000_000
    mm = (tick // 100_000) % 100
    ss = (tick // 1_000) % 100
    ms = tick % 1_000

    return (
            base_ts
            + (hh * 3600 + mm * 60 + ss) * 1_000_000
            + ms * 1_000
    )


# ============================
# 主入口（向量化）
# ============================
def parse_events(
        df: pd.DataFrame,
        kind: EventKind,
) -> pd.DataFrame:
    """
    输入：交易所原始 dataframe（一个 batch）
    输出：Internal canonical dataframe（未做 A 股过滤）
    """

    if df.empty:
        return df

    # ------------------------------------------------
    # 1) symbol（不裁决，只转字符串）
    # ------------------------------------------------
    out = pd.DataFrame()
    out["symbol"] = df["SecurityID"].astype(str).str.zfill(6)

    # ------------------------------------------------
    # 2) ts（向量化，绝不走 datetime.apply）
    # ------------------------------------------------
    # date 来自 TradeTime（batch 内恒定）
    date_str = df["TradeTime"].iloc[0][:10]

    if "TickTime" in df.columns:
        out["ts"] = build_ts_from_tick(
            date_str=date_str,
            tick=df["TickTime"],
        )
    elif "OrderTime" in df.columns:
        out["ts"] = build_ts_from_tick(
            date_str=date_str,
            tick=df["OrderTime"],
        )
    else:
        raise KeyError("TickTime / OrderTime 必须至少存在一个")

    # ------------------------------------------------
    # 3) 交易所判定（只用 ExchangeID，不用 symbol）
    # ------------------------------------------------
    exchange_id = int(df["ExchangeID"].iloc[0])

    # ============================================================
    # SH (ExchangeID = 1)
    # ============================================================
    if exchange_id == 1:
        if kind == "order":
            out["event"] = df["TickType"].map(
                {"A": "ADD", "D": "CANCEL"}
            )

            out["order_id"] = df["SubSeq"].astype("int64")
            out["side"] = df["Side"].map(
                {1: "B", 2: "S", "1": "B", "2": "S"}
            )
            out["price"] = df["Price"].astype("float64")
            out["volume"] = df["Volume"].astype("int64")
            out["buy_no"] = 0
            out["sell_no"] = 0

        else:  # trade
            # 只保留成交 TickType == 'T'
            mask = df["TickType"] == "T"
            if not mask.any():
                return out.iloc[0:0]

            out = out[mask]
            df2 = df.loc[mask]

            out["event"] = "TRADE"
            out["order_id"] = df2["SubSeq"].astype("int64")
            out["side"] = df2["Side"].map(
                {1: "B", 2: "S", "1": "B", "2": "S"}
            )
            out["price"] = df2["Price"].astype("float64")
            out["volume"] = df2["Volume"].astype("int64")
            out["buy_no"] = df2["BuyNo"].astype("int64")
            out["sell_no"] = df2["SellNo"].astype("int64")

    # ============================================================
    # SZ (ExchangeID = 2)
    # ============================================================
    elif exchange_id == 2:
        if kind == "order":
            out["event"] = df["OrderType"].map(
                {0: "CANCEL", 1: "ADD", 2: "ADD", 3: "ADD",
                 "0": "CANCEL", "1": "ADD", "2": "ADD", "3": "ADD"}
            )

            out["order_id"] = df["SubSeq"].astype("int64")
            out["side"] = df["Side"].map(
                {1: "B", 2: "S", "1": "B", "2": "S"}
            )
            out["price"] = df["Price"].astype("float64")
            out["volume"] = df["Volume"].astype("int64")
            out["buy_no"] = 0
            out["sell_no"] = 0

        else:  # trade
            out["event"] = df["ExecType"].map(
                {1: "TRADE", 2: "CANCEL", "1": "TRADE", "2": "CANCEL"}
            )

            out["order_id"] = df["SubSeq"].astype("int64")
            out["side"] = None
            out["price"] = df["TradePrice"].astype("float64")
            out["volume"] = df["TradeVolume"].astype("int64")
            out["buy_no"] = df["BuyNo"].astype("int64")
            out["sell_no"] = df["SellNo"].astype("int64")

    else:
        raise ValueError(f"未知 ExchangeID: {exchange_id}")

    # ------------------------------------------------
    # 4) 兜底：event 不能为空
    # ------------------------------------------------
    out = out[out["event"].notna()]

    return out[OUT_COLS]

# #!filepath: src/l2/common/event_parser.py
# from __future__ import annotations
#
# from typing import Dict, Literal, Tuple, Any
#
# import pandas as pd
#
# from src.utils.datetime_utils import DateTimeUtils as dt
# from src import logs
#
# # 内部统一事件 Schema（唯一真相）
# INTERNAL_SCHEMA = [
#     "ts",  # 标准化时间戳（datetime64[ns, Asia/Shanghai]）
#     "event",  # 'ADD' / 'CANCEL' / 'TRADE'
#     "order_id",  # 订单唯一标识（SubSeq / OrderNO 等）
#     "side",  # 'B' / 'S' / None
#     "price",  # 价格（float）
#     "volume",  # 数量（int）
#     "buy_no",  # 成交买方序号（无则 0）
#     "sell_no",  # 成交卖方序号（无则 0）
# ]
#
# EventKind = Literal["order", "trade"]
#
# # =============================================================================
# # 交易所字段映射配置（可扩展：SH=1, SZ=2, 未来可加 BJ=4, HK=3 等）
# # =============================================================================
# EXCHANGE_MAPPING: Dict[int, Dict[str, Any]] = {
#     # -------------------------------------------------------------------------
#     # 上海交易所 SH (ExchangeID = 1)
#     # -------------------------------------------------------------------------
#     1: {
#         # Order 事件
#         "order.time": "TickTime",
#         "order.event": ("TickType", {"A": "ADD", "D": "CANCEL"}),
#         "order.price": "Price",
#         "order.volume": "Volume",
#         "order.side": ("Side", {1: "B", 2: "S"}),
#         "order.id": "SubSeq",
#         "order.buy_no": None,
#         "order.sell_no": None,
#
#         # Trade 事件
#         "trade.time": "TickTime",
#         "trade.event": ("TickType", {"T": "TRADE"}),
#         "trade.price": "Price",
#         "trade.volume": "Volume",
#         "trade.side": ("Side", {1: "B", 2: "S"}),
#         "trade.id": "SubSeq",
#         "trade.buy_no": "BuyNo",
#         "trade.sell_no": "SellNo",
#     },
#
#     # -------------------------------------------------------------------------
#     # 深圳交易所 SZ (ExchangeID = 2)
#     # -------------------------------------------------------------------------
#     2: {
#         # Order 事件（逐笔委托）
#         "order.time": "OrderTime",
#         "order.event": ("OrderType", {0: "CANCEL", 1: "ADD", 2: "ADD", 3: "ADD"}),
#         "order.price": "Price",
#         "order.volume": "Volume",
#         "order.side": ("Side", {1: "B", 2: "S"}),
#         "order.id": "SubSeq",
#         "order.buy_no": None,
#         "order.sell_no": None,
#
#         # Trade 事件（逐笔成交）
#         "trade.time": "TickTime",
#         "trade.event": ("ExecType", {1: "TRADE", 2: "CANCEL"}),
#         "trade.price": "TradePrice",
#         "trade.volume": "TradeVolume",
#         "trade.side": None,  # SZ 成交侧不可靠，主要用价格+盘口推断
#         "trade.id": "SubSeq",
#         "trade.buy_no": "BuyNo",
#         "trade.sell_no": "SellNo",
#     },
# }
#
#
# # ================================================================
# # TradeTime → date
# # ================================================================
# def extract_date(trade_time):
#     return dt.extract_date(trade_time)
#
#
# # ================================================================
# # TickTime → (hh, mm, ss, μs)
# # ================================================================
# def parse_tick(tick):
#     return dt.parse_tick_time(tick)
#
#
# # ================================================================
# # 合成最终 ts
# # ================================================================
# def combine(date, tick_tuple):
#     return dt.combine_date_tick(date, tick_tuple)
#
# # ================================================================
# # datetime → int ts（微秒，唯一真相）
# # ================================================================
# def to_int_ts(dt_obj) -> int:
#     """
#     将 tz-aware datetime 转为 int 时间戳（微秒）
#     """
#     if not hasattr(dt_obj, "timestamp"):
#         raise TypeError(f"expect datetime, got {type(dt_obj)}")
#
#     return int(dt_obj.timestamp() * 1_000_000)
#
# # =============================================================================
# # 通用解析函数：df + exchange_id + kind → InternalEvent DataFrame
# # =============================================================================
# def parse_events(
#         df: pd.DataFrame,
#         # exchange_id: int,
#         kind: EventKind,
# ) -> pd.DataFrame:
#     """
#     统一解析 SH / SZ 的 Order / Trade 表结构
#     输出 InternalEvent:
#         ts (真正的 datetime)
#         event (ADD / CANCEL / TRADE)
#         order_id
#         side
#         price
#         volume
#         buy_no
#         sell_no
#     """
#     if "TickTime" not in df.columns and 'OrderTime' not in df.columns:
#         raise KeyError("供应商必须提供 TickTime & OrderTime 字段 (HHMMSSmmm)")
#     df = df.copy()
#
#     # 1) TradeTime → date
#     date = extract_date(df["TradeTime"].iloc[0])
#
#     # 2) TickTime → 精确事件时间
#     if 'TickTime' in df.columns:
#         df["ts"] = df["TickTime"].apply(lambda x: to_int_ts(combine(date, parse_tick(x))))
#     elif 'OrderTime' in df.columns:
#         df["ts"] = df["OrderTime"].apply(lambda x: to_int_ts(combine(date, parse_tick(x))))
#     else:
#         raise KeyError(f'No TickTime or OrderTime in {df.columns}')
#
#     # ------------------------------------------------------------------
#     # 3) 交易所结构差异适配
#     # ------------------------------------------------------------------
#     # exchange_id = infer_exchange_id(df["SecurityID"].iloc[0])
#     exchange_id = int(df["ExchangeID"].iloc[0])
#
#     if exchange_id == 1:
#         out =_parse_sh(df, kind)
#     elif exchange_id == 2:
#         out = _parse_sz(df, kind)
#     else:
#         raise ValueError(f"未知交易所 ID: {exchange_id}")
#
#     # =========================================================
#     # 🔥 最终兜底：任何交易所、任何 kind，都不允许 event 泄漏为 NaN/非法
#     # =========================================================
#     out = out.copy()
#     out["event"] = out["event"].astype("string")
#     out = out[out["event"].notna()]
#     # out = out[out["event"].isin(VALID_EVENTS)]
#
#     return out
#
#
# # ============================================================
# # 下面是两个 exchange parser
# # ============================================================
#
# def _parse_sh(df: pd.DataFrame, kind: str) -> pd.DataFrame:
#     df['symbol'] = df["SecurityID"]
#     if kind == "order":
#
#         df["event"] = df["TickType"].map({"A": "ADD", "D": "CANCEL", "T": "TRADE"})
#         df["order_id"] = df["SubSeq"].astype(int)
#         df["side"] = df["Side"].map({'1': "B", '2': "S"})
#         df["price"] = df["Price"].astype(float)
#         df["volume"] = df["Volume"].astype(int)
#         df["buy_no"] = df["BuyNo"]
#         df["sell_no"] = df["SellNo"]
#
#     else:  # trade
#         df = df[df["TickType"] == "T"]
#         df["event"] = "TRADE"
#         df["order_id"] = df["SubSeq"].astype(int)
#         df["side"] = df["Side"].map({'1': "B", '2': "S"})
#         df["price"] = df["Price"]
#         df["volume"] = df["Volume"]
#         df["buy_no"] = df["BuyNo"]
#         df["sell_no"] = df["SellNo"]
#
#     return df[['symbol', "ts", "event", "order_id", "side", "price", "volume", "buy_no", "sell_no"]]
#
#
# def _parse_sz(df: pd.DataFrame, kind: str) -> pd.DataFrame:
#     df['symbol'] = df["SecurityID"]
#
#     if kind == "order":
#         df["event"] = df["OrderType"].map({'0': "CANCEL", '1': "ADD", '2': "ADD", '3': "ADD"})
#
#         df["side"] = df["Side"].map({'1': "B", '2': "S"})
#         df["order_id"] = df["SubSeq"]
#         df["price"] = df["Price"]
#         df["volume"] = df["Volume"]
#         df["buy_no"] = 0
#         df["sell_no"] = 0
#
#     else:  # trade
#         df["event"] = df["ExecType"].map({'1': "TRADE", '2': "CANCEL"})
#         df["order_id"] = df["SubSeq"]
#         df["side"] = None
#         df["price"] = df["TradePrice"]
#         df["volume"] = df["TradeVolume"]
#         df["buy_no"] = df["BuyNo"]
#         df["sell_no"] = df["SellNo"]
#
#     return df[['symbol',"ts", "event", "order_id", "side", "price", "volume", "buy_no", "sell_no"]]
#
#
# def infer_exchange_id(symbol: str|int) -> int:
#     """
#     根据 A 股 symbol 自动推断交易所 ID:
#         1 = SH（上海）
#         2 = SZ（深圳）
#
#     规则：
#         SH: 600/601/603/605/688/689
#         SZ: 000/001/002/003/300/301
#     """
#
#     s = str(symbol).strip()
#     s = s.zfill(6)
#
#     if s.startswith(("60", "688")):
#         return 1
#     if s.startswith(("00", "30")):
#         return 2
#
#     raise ValueError(f"[infer_exchange_id] 无法根据 symbol 推断交易所: {symbol}")
