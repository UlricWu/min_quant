from __future__ import annotations

import pandas as pd
import pyarrow as pa


class FeatureL1NormEngine:
    """
    FeatureL1NormEngine (Frozen v1.0)

    ==========================================================
    Role (Frozen)
    ==========================================================

    L1-B: Statistical Normalization Layer

    This engine performs *ONLY* statistical normalization
    over time-series features.

    It does NOT define new economic variables.
    It does NOT create new temporal structure.
    It does NOT implement trading indicators.

    ==========================================================
    What this engine IS responsible for
    ==========================================================

    - Rolling z-score
    - (Optionally future) rank / clip / scale
    - Stabilizing feature distributions
    - Improving cross-symbol comparability
    - Strict no-leak normalization

    ==========================================================
    What this engine is NOT allowed to do
    ==========================================================

    - Rolling mean / sum / std as output features
    - Return / momentum / ratio
    - Any feature with directional interpretation
    - Technical indicators (RSI, MACD, etc.)
    - Any form of strategy logic

    ==========================================================
    Input Contract
    ==========================================================

    - Single-symbol Arrow Table
    - Rows sorted by time
    - Contains numeric feature columns:
        - l0_*
        - l1_*  (from FeatureL1StatEngine)
    - No cross-symbol operations

    ==========================================================
    Output Contract
    ==========================================================

    - Same number of rows
    - Append or replace l1_norm_* columns
    - All statistics use history [t-window ... t-1]
    - No NaN / Inf in final output
    - Deterministic & reproducible

    ==========================================================
    Window Semantics (Frozen)
    ==========================================================

    window = number of historical rows (FeatureL0 rows)

    window is part of feature schema:
        FeatureL1Norm(window=20) != FeatureL1Norm(window=60)

    ==========================================================
    Naming Convention (Frozen)
    ==========================================================

    Output columns:

        l1_z_w{window}_{base_feature_name}

    Examples:

        l1_z_w20_volume
        l1_z_w60_range
        l1_z_w20_ratio_volume
    """

    # --------------------------------------------------
    def __init__(
        self,
        window: int,
        *,
        include_l0: bool = True,
        include_l1: bool = True,
    ) -> None:
        if window <= 0:
            raise ValueError("window must be positive")

        self.window = window
        self._wtag = f"w{window}"

        self.include_l0 = include_l0
        self.include_l1 = include_l1

    # --------------------------------------------------
    def execute(self, table: pa.Table) -> pa.Table:
        if table.num_rows == 0:
            return table

        self._validate_input(table)

        # Narrow, controlled Arrow -> pandas conversion
        df = table.to_pandas()

        # Normalize eligible columns
        for col in self._candidate_columns(df):
            self._add_zscore(df, col)

        # pandas -> Arrow (schema evolves by column append)
        return pa.Table.from_pandas(df, preserve_index=False)

    # ==================================================
    # Validation
    # ==================================================
    @staticmethod
    def _validate_input(table: pa.Table) -> None:
        if not any(
            c.startswith("l0_") or c.startswith("l1_")
            for c in table.column_names
        ):
            raise ValueError(
                "FeatureL1NormEngine requires l0_* or l1_* features"
            )

    # ==================================================
    # Column selection
    # ==================================================
    def _candidate_columns(self, df: pd.DataFrame) -> list[str]:
        """
        Select columns eligible for normalization.

        Rules (Frozen):
        - Numeric only
        - No existing l1_z_* columns
        - Respect include_l0 / include_l1 flags
        """
        cols: list[str] = []

        for c in df.columns:
            if c.startswith("l1_z_"):
                continue

            if c.startswith("l0_") and not self.include_l0:
                continue

            if c.startswith("l1_") and not self.include_l1:
                continue

            if not pd.api.types.is_numeric_dtype(df[c]):
                continue

            cols.append(c)

        return cols

    # ==================================================
    # Z-score logic (core)
    # ==================================================
    def _add_zscore(self, df: pd.DataFrame, col: str) -> None:
        """
        z_t = (x_t - mean(x[t-window : t-1])) / std(x[t-window : t-1])

        - Strict no-leak (shift + rolling)
        - std == 0 → 0.0
        - NaN → 0.0
        """

        x = df[col]

        hist_mean = (
            x.shift(1)
            .rolling(self.window, min_periods=self.window)
            .mean()
        )

        hist_std = (
            x.shift(1)
            .rolling(self.window, min_periods=self.window)
            .std()
        )

        z = (x - hist_mean) / hist_std

        out_col = f"l1_z_{self._wtag}_{col}"

        df[out_col] = (
            z.fillna(0.0)
            .where(hist_std != 0, 0.0)
        )


# ======================================================
# Utility
# ======================================================
def _append_or_replace(table: pa.Table, name: str, arr: pa.Array) -> pa.Table:
    if name in table.column_names:
        idx = table.column_names.index(name)
        return table.set_column(idx, name, arr)
    return table.append_column(name, arr)


def shift_1(x: pa.Array | pa.ChunkedArray) -> pa.Array:
    """
    Arrow-safe shift(1):
      out[0]  = null
      out[i]  = x[i-1]
    Compatible with Apache Arrow v22.x
    """
    n = len(x)
    if n == 0:
        return x

    # 保证是 Array（不是 ChunkedArray）
    if isinstance(x, pa.ChunkedArray):
        x = x.combine_chunks()

    return pa.concat_arrays([
        pa.nulls(1, type=x.type),
        x.slice(0, n - 1),
    ])
