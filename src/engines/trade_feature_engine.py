#!filepath: src/engines/trade_feature_engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable

import numpy as np
import pandas as pd
@dataclass(frozen=True)
class TradeFeatureConfig:
    # aggressor
    tick_rule_carry: bool = True

    # impact
    impact_mode: str = "bps"     # "bps" | "ticks"
    tick_size: float = 0.01

    # VPIN
    vpin_bucket_volume: int = 50_000
    vpin_rolling: int = 50

    # trade bucket (by notional)
    small_q: float = 0.70
    large_q: float = 0.95

class TradeFeatureEngine:
    """
    TradeFeatureEngine（研究 / 特征层）

    输入：trade-only DataFrame（已 normalize / enrich）
    输出：新增特征列（不覆盖原字段）
    """

    def __init__(self, cfg: TradeFeatureConfig) -> None:
        self.cfg = cfg

    # ======================================================
    # 主入口（offline / research）
    # ======================================================
    def enrich(
        self,
        trade_df: pd.DataFrame,
        l1_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        trade_df: 必须包含 ts, price, volume
        l1_df: 可选，包含 ts, bid_px_1, ask_px_1
        """
        df = trade_df.sort_values("ts").reset_index(drop=True).copy()

        # 1️⃣ aggressor
        df["aggressor"] = self._infer_aggressor(df)

        # 2️⃣ trade bucket（by notional）
        df["trade_bucket"] = self._bucket_by_notional(df)

        # 3️⃣ impact（需要 L1）
        if l1_df is not None:
            df = self._merge_l1(df, l1_df)
            df["is_price_impact"], df["impact"] = self._compute_impact(df)
        else:
            df["is_price_impact"] = False
            df["impact"] = 0.0

        # 4️⃣ VPIN
        df["vpin"] = self._compute_vpin(df)

        return df
    def _infer_aggressor(self, df: pd.DataFrame) -> pd.Series:
        """
        Tick rule:
        price > prev -> B
        price < prev -> S
        equal -> carry last (optional)
        """
        price = df["price"].astype(float)
        prev = price.shift(1)

        aggr = pd.Series(index=df.index, dtype="object")
        aggr[price > prev] = "B"
        aggr[price < prev] = "S"

        if self.cfg.tick_rule_carry:
            aggr = aggr.ffill()

        return aggr
    def _bucket_by_notional(self, df: pd.DataFrame) -> pd.Series:
        notional = df["price"] * df["volume"]
        q_small = notional.quantile(self.cfg.small_q)
        q_large = notional.quantile(self.cfg.large_q)

        bucket = pd.Series(index=df.index, dtype="object")
        bucket[notional <= q_small] = "S"
        bucket[(notional > q_small) & (notional <= q_large)] = "M"
        bucket[notional > q_large] = "L"
        return bucket
    def _merge_l1(self, trade_df: pd.DataFrame, l1_df: pd.DataFrame) -> pd.DataFrame:
        """
        最近时间对齐（asof）
        """
        return pd.merge_asof(
            trade_df.sort_values("ts"),
            l1_df.sort_values("ts"),
            on="ts",
            direction="backward",
        )
    def _compute_impact(self, df: pd.DataFrame):
        mid = (df["bid_px_1"] + df["ask_px_1"]) / 2.0

        is_impact = (
            ((df["aggressor"] == "B") & (df["price"] >= df["ask_px_1"])) |
            ((df["aggressor"] == "S") & (df["price"] <= df["bid_px_1"]))
        )

        if self.cfg.impact_mode == "ticks":
            impact = np.where(
                df["aggressor"] == "B",
                (df["price"] - df["ask_px_1"]) / self.cfg.tick_size,
                (df["bid_px_1"] - df["price"]) / self.cfg.tick_size,
            )
        else:
            impact = (df["price"] - mid).abs() / mid * 10_000

        impact = impact.clip(lower=0).fillna(0.0)
        return is_impact, impact
    def _compute_vpin(self, df: pd.DataFrame) -> pd.Series:
        V = self.cfg.vpin_bucket_volume
        vol = df["volume"].astype(int)

        bucket_id = (vol.cumsum() // V).astype(int)
        df["_vpin_bucket"] = bucket_id

        buy = np.where(df["aggressor"] == "B", vol, 0)
        sell = np.where(df["aggressor"] == "S", vol, 0)

        agg = (
            pd.DataFrame({"buy": buy, "sell": sell, "bucket": bucket_id})
            .groupby("bucket")
            .sum()
        )

        imbalance = (agg["buy"] - agg["sell"]).abs() / (agg["buy"] + agg["sell"])
        vpin = imbalance.rolling(self.cfg.vpin_rolling, min_periods=1).mean()

        return bucket_id.map(vpin).fillna(0.0)
