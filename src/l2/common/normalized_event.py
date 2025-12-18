# from __future__ import annotations
#
# from typing import Iterable
# import pandas as pd
#
# from src.l2.common.exchange_def import ExchangeDefinition
# from src.utils.datetime_utils import DateTimeUtils as dt
#
#!filepath: src/l2/common/normalized_event.py
from __future__ import annotations
from dataclasses import dataclass

from dataclasses import dataclass, asdict
@dataclass(slots=True)
class NormalizedEvent:
    """
    内部统一事件（唯一真相）
    """
    ts: int
    event: str           # ADD / CANCEL / TRADE
    order_id: int
    side: str | None     # B / S / None
    price: float
    volume: int
    buy_no: int
    sell_no: int

    @classmethod
    def from_row(cls, row):
        return cls(
            ts=int(row.ts),
            event=row.event,
            order_id=int(row.order_id),
            side=None if row.side != row.side else row.side,
            price=float(row.price),
            volume=int(row.volume),
            buy_no=int(row.buy_no),
            sell_no=int(row.sell_no),
        )

    # ✅ 关键补充
    def to_dict(self) -> dict:
        """
        用于 parquet / dataframe 序列化
        """
        return asdict(self)

# # ============================================================================
# # 内部统一事件 Schema（唯一真相）
# # ============================================================================
# INTERNAL_COLUMNS = [
#     "ts",        # datetime64[ns, Asia/Shanghai]
#     "event",     # 'ADD' / 'CANCEL' / 'TRADE'
#     "order_id",  # int
#     "side",      # 'B' / 'S' / None
#     "price",     # float
#     "volume",    # int
#     "buy_no",    # int
#     "sell_no",   # int
# ]
#
#
# # ============================================================================
# # 核心：纯规范化函数
# # ============================================================================
# def normalize_events(
#     df: pd.DataFrame,
#     *,
#     definition: ExchangeDefinition,
#     trade_date,
# ) -> pd.DataFrame:
#     """
#     将交易所原始逐笔数据规范化为 InternalEvent DataFrame。
#
#     参数：
#     - df            : 原始逐笔 DataFrame（SH / SZ / Order / Trade 任意）
#     - definition    : ExchangeDefinition（由 Engine 从 registry 选择）
#     - trade_date    : 交易日期（date / str / datetime 均可）
#
#     返回：
#     - DataFrame，列为 INTERNAL_COLUMNS
#     """
#
#     if df.empty:
#         return pd.DataFrame(columns=INTERNAL_COLUMNS)
#
#     df = df.copy()
#
#     # ------------------------------------------------------------------
#     # 1) 时间规范化：TickTime / OrderTime → ts
#     # ------------------------------------------------------------------
#     time_field = definition.time_field
#     if time_field not in df.columns:
#         raise KeyError(f"Missing time field: {time_field}")
#
#     # trade_date → date
#     date = dt.extract_date(trade_date)
#
#     df["ts"] = df[time_field].apply(
#         lambda x: dt.combine_date_tick(date, dt.parse_tick_time(x))
#     )
#
#     # ------------------------------------------------------------------
#     # 2) 事件类型映射
#     # ------------------------------------------------------------------
#     event_field = definition.event_field
#     if event_field not in df.columns:
#         raise KeyError(f"Missing event field: {event_field}")
#
#     df["event"] = df[event_field].map(definition.event_mapping)
#
#     # 丢弃无法映射的事件（如非 TRADE / 非 ADD / CANCEL）
#     df = df[df["event"].notna()]
#
#     # ------------------------------------------------------------------
#     # 3) 基础字段映射
#     # ------------------------------------------------------------------
#     df["order_id"] = df[definition.id_field].astype(int)
#     df["price"] = df[definition.price_field].astype(float)
#     df["volume"] = df[definition.volume_field].astype(int)
#
#     # ------------------------------------------------------------------
#     # 4) side 处理（部分交易所可能为 None）
#     # ------------------------------------------------------------------
#     if definition.side_field is not None:
#         df["side"] = df[definition.side_field].map(definition.side_mapping)
#     else:
#         df["side"] = None
#
#     # ------------------------------------------------------------------
#     # 5) 买卖方序号（可选）
#     # ------------------------------------------------------------------
#     if definition.buy_no_field is not None:
#         df["buy_no"] = df[definition.buy_no_field].fillna(0).astype(int)
#     else:
#         df["buy_no"] = 0
#
#     if definition.sell_no_field is not None:
#         df["sell_no"] = df[definition.sell_no_field].fillna(0).astype(int)
#     else:
#         df["sell_no"] = 0
#
#     # ------------------------------------------------------------------
#     # 6) 输出 InternalEvent（唯一真相）
#     # ------------------------------------------------------------------
#     return df[INTERNAL_COLUMNS]
