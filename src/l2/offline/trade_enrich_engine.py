#!filepath: src/l2/offline/trade_enrich_engine.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class TradeEnrichConfig:
    """
    对逐笔成交增加：
    - burst_id：按时间窗口聚簇
    - is_price_impact：价格是否跳变
    """
    burst_window_ms: int = 5


class TradeEnrichEngine:
    """
    DataFrame 版逐笔成交增强引擎：
    - 输入：parse_events 之后的 DataFrame（必须有 ts, price, volume 等列）
    - 输出：在原 DataFrame 基础上新增 burst_id, is_price_impact
    """

    def __init__(self, cfg: Optional[TradeEnrichConfig] = None) -> None:
        self.cfg = cfg or TradeEnrichConfig()

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        要求 df 至少包含：
            - ts: datetime64[ns]（带不带 tz 都可以）
            - price: float
        """
        if df.empty:
            df = df.copy()
            df["burst_id"] = pd.Series(dtype="int64")
            df["is_price_impact"] = pd.Series(dtype="bool")
            return df

        df = df.copy()

        # -------- 按 ts 排序（防御性，防止上游写 parquet 时乱序） --------
        df = df.sort_values("ts")

        # -------- 计算 burst_id --------
        ts_index = pd.DatetimeIndex(df["ts"])
        ts_ns = ts_index.view("int64")  # datetime64[ns] → int64 ns

        # 相邻事件的时间差
        dt_ns = ts_ns - ts_ns[0]
        dt_diff = np.diff(ts_ns, prepend=ts_ns[0])

        burst_window_ns = self.cfg.burst_window_ms * 1_000_000  # ms → ns
        new_burst = dt_diff > burst_window_ns
        burst_id = new_burst.cumsum()
        df["burst_id"] = burst_id.astype("int64")

        # -------- 计算 is_price_impact --------
        price = df["price"].astype(float).to_numpy()
        prev_price = np.roll(price, 1)
        prev_price[0] = price[0]  # 第一笔没有对比对象

        is_impact = price != prev_price
        is_impact[0] = False
        df["is_price_impact"] = is_impact

        return df
