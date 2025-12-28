from __future__ import annotations

import pyarrow as pa
import pandas as pd


class FeatureL1Engine:
    """
    FeatureL1Engine (Frozen v1.2, pandas-based, window-aware)

    ===========================
    Why pandas (Design Rationale)
    ===========================

    This engine intentionally uses pandas (not pyarrow.compute) for L1 features.

    Rationale:

    1. FeatureL1 is a *time-series statistical layer* (rolling / z-score),
       not a columnar ETL layer.
       Pandas provides mature, C-optimized rolling kernels with
       well-defined semantics (shift + rolling).

    2. As of pyarrow v23, Arrow does NOT provide native rolling mean/std
       kernels suitable for time-series feature engineering.
       Forcing Arrow-only implementation would require:
         - Python loops, or
         - pandas fallback anyway, or
         - unstable / non-existent APIs
       all of which are worse than an explicit pandas boundary.

    3. In this system, pandas usage is:
         - strictly limited to FeatureL1
         - offline
         - single-symbol
         - sequential rolling
         - bounded scale (~240 rows/day * ~5000 symbols)
       This is a *safe and controlled* usage scenario.

    4. Heavy computation (normalize / enrich / L0 features)
       has already been completed using pyarrow compute.
       L1 adds only lightweight statistical transforms.

    Architectural Boundary (Frozen):
      - Arrow: schema, I/O, L0 vectorized features
      - Pandas: L1 rolling time-series statistics
      - NumPy / ML: downstream modeling

    This separation is intentional and should NOT be removed
    unless Arrow gains stable rolling kernels in the future.

    ===========================
    Functional Contract
    ===========================

    Input:
      - Single-symbol FeatureL0 Arrow Table
      - Rows sorted by time
      - l0_* feature columns present

    Output:
      - Same number of rows
      - Append / replace L1 feature columns
      - Strictly no future leakage:
            stats use [t-window ... t-1]
            value normalized at t

    Naming:
      - l1_z_w{window}_{feature}

    """

    def __init__(self, window: int=20) -> None:
        if window <= 0:
            raise ValueError("window must be positive")
        self.window = window
        self._wtag = f"w{window}"

    # --------------------------------------------------
    def execute(self, table: pa.Table) -> pa.Table:
        if table.num_rows == 0:
            return table

        self._validate_input(table)

        # Arrow -> pandas (narrow, controlled conversion)
        df = table.to_pandas()

        # rolling z-score for all l0_* features
        for col in self._l0_columns(df):
            self._add_zscore(df, col)

        # pandas -> Arrow (schema-preserving)
        return pa.Table.from_pandas(df, preserve_index=False)

    # ==================================================
    # Validation
    # ==================================================
    @staticmethod
    def _validate_input(table: pa.Table) -> None:
        if not any(c.startswith("l0_") for c in table.column_names):
            raise ValueError("FeatureL1 requires L0 features (l0_*)")

    @staticmethod
    def _l0_columns(df: pd.DataFrame) -> list[str]:
        return [c for c in df.columns if c.startswith("l0_")]

    # ==================================================
    # Feature logic
    # ==================================================
    def _add_zscore(self, df: pd.DataFrame, col: str) -> None:
        """
        z_t = (x_t - mean(x[t-window : t-1])) / std(x[t-window : t-1])
        """

        x = df[col]

        mean = (
            x.shift(1)
            .rolling(self.window, min_periods=self.window)
            .mean()
        )

        std = (
            x.shift(1)
            .rolling(self.window, min_periods=self.window)
            .std()
        )

        z = (x - mean) / std

        out_col = f"l1_z_{self._wtag}_{col[3:]}"
        df[out_col] = (
            z.fillna(0.0)
            .where(std != 0, 0.0)
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
