from __future__ import annotations

import math
from typing import Iterable

import pandas as pd
import pyarrow as pa


class FeatureL1StatEngine:
    """
    FeatureL1StatEngine (Frozen v1.0)

    ======================================
    Role
    ======================================

    L1-A: Statistical Expression Layer

    - Define NEW temporal statistical variables
    - Deterministic, causal, no-leak
    - No normalization (z-score / rank)
    - No technical indicators
    - No strategy assumptions

    ======================================
    Input Contract
    ======================================

    - Single-symbol FeatureL0 Arrow Table
    - Rows sorted by time
    - l0_* feature columns present

    ======================================
    Output Contract
    ======================================

    - Same number of rows
    - Append / replace l1_* columns
    - All statistics use [t-window ... t-1]
    - No NaN in final output

    ======================================
    Window Semantics (Frozen)
    ======================================

    window = number of historical rows (FeatureL0 rows)

    window is part of feature schema:
      FeatureL1Stat(window=20) != FeatureL1Stat(window=60)
    """

    # --------------------------------------------------
    def __init__(
        self,
        window: int,
        *,
        enable_return: bool = True,
        enable_volume_ratio: bool = True,
        enable_range_ratio: bool = True,
    ) -> None:
        if window <= 0:
            raise ValueError("window must be positive")

        self.window = window
        self._wtag = f"w{window}"

        self.enable_return = enable_return
        self.enable_volume_ratio = enable_volume_ratio
        self.enable_range_ratio = enable_range_ratio

    # --------------------------------------------------
    def execute(self, table: pa.Table) -> pa.Table:
        if table.num_rows == 0:
            return table

        self._validate_input(table)

        # Arrow → pandas (controlled, single-symbol, small)
        df = table.to_pandas()

        # ==================================================
        # 1. Rolling statistics
        # ==================================================
        self._add_rolling_mean(df)
        self._add_rolling_sum(df)
        self._add_rolling_std(df)

        # ==================================================
        # 2. Return-like features
        # ==================================================
        if self.enable_return:
            self._add_log_return(df)

        # ==================================================
        # 3. Ratio-like features
        # ==================================================
        if self.enable_volume_ratio:
            self._add_volume_ratio(df)

        if self.enable_range_ratio:
            self._add_range_ratio(df)

        # pandas → Arrow
        return pa.Table.from_pandas(df, preserve_index=False)

    # ==================================================
    # Validation
    # ==================================================
    @staticmethod
    def _validate_input(table: pa.Table) -> None:
        if not any(c.startswith("l0_") for c in table.column_names):
            raise ValueError("FeatureL1Stat requires L0 features (l0_*)")

    # ==================================================
    # Rolling statistics
    # ==================================================
    def _add_rolling_mean(self, df: pd.DataFrame) -> None:
        if "l0_volume" in df:
            m = (
                df["l0_volume"]
                .shift(1)
                .rolling(self.window, min_periods=self.window)
                .mean()
            )
            df[f"l1_mean_{self._wtag}_volume"] = m.fillna(0.0)

    def _add_rolling_sum(self, df: pd.DataFrame) -> None:
        if "l0_trade_count" in df:
            s = (
                df["l0_trade_count"]
                .shift(1)
                .rolling(self.window, min_periods=self.window)
                .sum()
            )
            df[f"l1_sum_{self._wtag}_trade_count"] = s.fillna(0.0)

    def _add_rolling_std(self, df: pd.DataFrame) -> None:
        if "l0_abs_move" in df:
            s = (
                df["l0_abs_move"]
                .shift(1)
                .rolling(self.window, min_periods=self.window)
                .std()
            )
            df[f"l1_std_{self._wtag}_abs_move"] = s.fillna(0.0)

    # ==================================================
    # Return-like
    # ==================================================
    def _add_log_return(self, df: pd.DataFrame) -> None:
        """
        log_ret_t = log(close_t / close_{t-1})
        """
        if "close" not in df:
            return

        prev = df["close"].shift(1)
        ret = (df["close"] / prev).apply(
            lambda x: math.log(x) if x > 0 else 0.0
        )

        df[f"l1_ret_{self._wtag}_1"] = ret.fillna(0.0)

    # ==================================================
    # Ratio-like
    # ==================================================
    def _add_volume_ratio(self, df: pd.DataFrame) -> None:
        if "l0_volume" not in df:
            return

        mean = (
            df["l0_volume"]
            .shift(1)
            .rolling(self.window, min_periods=self.window)
            .mean()
        )

        ratio = df["l0_volume"] / mean
        df[f"l1_ratio_{self._wtag}_volume"] = (
            ratio.replace([math.inf, -math.inf], 0.0)
            .fillna(0.0)
        )

    def _add_range_ratio(self, df: pd.DataFrame) -> None:
        if "l0_range" not in df:
            return

        mean = (
            df["l0_range"]
            .shift(1)
            .rolling(self.window, min_periods=self.window)
            .mean()
        )

        ratio = df["l0_range"] / mean
        df[f"l1_ratio_{self._wtag}_range"] = (
            ratio.replace([math.inf, -math.inf], 0.0)
            .fillna(0.0)
        )
