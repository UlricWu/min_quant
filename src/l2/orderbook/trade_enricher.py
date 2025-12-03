#!filepath: src/l2/trade_enricher.py
from __future__ import annotations

from typing import Union, Optional

import pandas as pd
import numpy as np
from dataclasses import dataclass
from pathlib import Path


@dataclass
class TradeEnricher:
    """
    Level-2 逐笔成交增强模块（Trade Enricher）

    ----
    特征包括：
    - Aggressor 主动买卖方向（B/S）
    - is_price_impact 是否吃穿盘口
    - impact 数值化的价格冲击
    - trade_bucket 大/中/小单分桶
    - burst_id 成交簇 ID（微观结构核心特征）
    - vpin VPIN (Volume-Synchronized PIN)

    ----
    设计原则：
    1. enrich() = 纯业务逻辑（df → df）
    2. enrich_file() = 辅助 IO，不在 pipeline 使用
    """

    large_trade_pct: float = 0.90      # 大单判定：>90 分位
    medium_trade_pct: float = 0.50     # 中单判定：>50 分位
    burst_window_ms: int = 30          # 成交簇窗口（毫秒）
    vpin_bucket_volume: int = 50_000   # VPIN 每桶体积

    # ---------------------------------------------------------
    # 主入口：增强所有特征
    # ---------------------------------------------------------
    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        # df = df.copy()

        # 时间戳必须在 ts 列
        df = df.sort_values("ts").reset_index(drop=True)

        # 买卖方向推断
        df["aggressor"] = df.apply(self._infer_aggressor, axis=1)

        # 盘口吃穿
        df["is_price_impact"] = df.apply(self._impact_flag, axis=1)

        # 价格冲击 impact（微结构）
        df["impact"] = self._compute_impact(df)

        # 成交尺寸分桶
        df["trade_bucket"] = self._bucket_trade_size(df)

        # 成交簇特征
        df["burst_id"] = self._compute_burst_id(df)

        # VPIN
        df["vpin"] = self._vpin(df)

        return df

    # ---------------------------------------------------------
    # 1. 买卖方向：Aggressor 推断
    # ---------------------------------------------------------
    @staticmethod
    def _infer_aggressor(row):
        """
        基于最简单可靠的 microstructure logic:
        - 若成交价 >= AskPrice1 → 主动买（B）
        - 若成交价 <= BidPrice1 → 主动卖（S）
        - 若在中间 → 取 PreTradePrice：若上行→B，下行→S
        """

        p = row["TradePrice"]
        bid = row["BidPrice1"]
        ask = row["AskPrice1"]

        if pd.notna(ask) and p >= ask:
            return "B"
        if pd.notna(bid) and p <= bid:
            return "S"

        # 中间成交
        return "B" if "PreTradePrice" in row and p >= row["PreTradePrice"] else "S"

    # ---------------------------------------------------------
    # 2. 是否吃穿 L1（是否突破盘口）
    # ---------------------------------------------------------
    @staticmethod
    def _impact_flag(row):
        """成交价突破 L1 的 ask/bid 判定为 impact"""
        p = row["TradePrice"]
        bid = row["BidPrice1"]
        ask = row["AskPrice1"]

        if pd.notna(ask) and p > ask:
            return True
        if pd.notna(bid) and p < bid:
            return True
        return False

    # ---------------------------------------------------------
    # 3. impact 数值化（价格冲击）
    # ---------------------------------------------------------
    @staticmethod
    def _compute_impact(df):
        """impact = (成交价 - midprice) / midprice"""
        mid = (df["BidPrice1"] + df["AskPrice1"]) / 2
        impact = (df["TradePrice"] - mid) / mid
        return impact.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    # ---------------------------------------------------------
    # 4. Bucket：小单/中单/大单
    # ---------------------------------------------------------
    def _bucket_trade_size(self, df):
        """
        基于成交量分位数划分：
        Small / Medium / Large
        """
        small_thr = df["TradeVolume"].quantile(self.medium_trade_pct)
        large_thr = df["TradeVolume"].quantile(self.large_trade_pct)

        def bucket(v):
            if v >= large_thr:
                return "L"
            elif v >= small_thr:
                return "M"
            return "S"

        return df["TradeVolume"].apply(bucket)

    # ---------------------------------------------------------
    # 5. 成交簇（burst）
    # ---------------------------------------------------------
    def _compute_burst_id(self, df: pd.DataFrame):
        """
        若连续成交间隔 ≤ burst_window_ms → 属于同一簇
        """
        ts = df["ts"].astype("int64") // 1_000_000  # 转毫秒
        diff = ts.diff().fillna(0)

        # 符合微结构定义：若间隔 > window 则开新簇
        burst_id = (diff > self.burst_window_ms).cumsum()
        return burst_id

    # ---------------------------------------------------------
    # 6. VPIN（Volume-Synchronized PIN）
    # ---------------------------------------------------------
    def _vpin(self, df: pd.DataFrame):
        """
        VPIN: |BuyVol - SellVol| / BucketVolume
        """

        df = df.copy()
        df["buy_vol"] = np.where(df["aggressor"] == "B", df["TradeVolume"], 0)
        df["sell_vol"] = np.where(df["aggressor"] == "S", df["TradeVolume"], 0)

        bucket = []
        curr_b = 0
        curr_s = 0
        out = []

        for b, s in zip(df["buy_vol"], df["sell_vol"]):
            curr_b += b
            curr_s += s

            if curr_b + curr_s >= self.vpin_bucket_volume:
                vpin = abs(curr_b - curr_s) / (curr_b + curr_s)
                out.append(vpin)
                curr_b = 0
                curr_s = 0
            else:
                out.append(np.nan)

        return pd.Series(out)

    def enrich_file(
        self,
        in_path: Union[str, Path],
        out_path: Optional[Union[str, Path]] = None,
        engine: str = "auto",
    ) -> pd.DataFrame:
        """
        辅助方法：直接从 parquet 路径读取 → enrich → 可选写回 parquet。
        不建议 pipeline 使用（Pipeline 应该手动处理 IO），
        但 notebook/临时回测非常方便。

        参数:
            in_path: 输入 parquet 路径
            out_path: 若提供，则将结果写入该 parquet 路径
            engine: pandas.read_parquet 引擎（auto/pyarrow/fastparquet）

        返回:
            rich_df: 增强后的 DataFrame
        """
        in_path = Path(in_path)
        if not in_path.exists():
            raise FileNotFoundError(f"[TradeEnricher] 输入 parquet 不存在: {in_path}")

        df = pd.read_parquet(in_path, engine=engine)
        enriched = self.enrich(df)

        if out_path is not None:
            out_path = Path(out_path)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            enriched.to_parquet(out_path, index=False)
        return enriched

