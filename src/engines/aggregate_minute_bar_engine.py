# src/engines/aggregate_minute_bar_engine.py

from __future__ import annotations

import pandas as pd


class AggregateMinuteBarEngine:
    """
    AggregateMinuteBarEngine

    Contract:
    - Input:
        DataFrame with columns:
            ts: int (ms, sorted ascending)
            price: float > 0
            volume: int > 0
    - Output:
        DataFrame with minute-level bars
    - No IO
    - No datetime
    - Deterministic
    """

    REQUIRED_COLUMNS = ("ts", "price", "volume")
    INTERVAL_MS = 60_000

    def run(self, trade_df: pd.DataFrame) -> pd.DataFrame:
        """
        Aggregate normalized trade data into minute bars.

        Parameters
        ----------
        trade_df : pd.DataFrame
            Must satisfy the input contract.

        Returns
        -------
        pd.DataFrame
            Minute bar DataFrame satisfying the output contract.
        """

        # Empty input → empty output
        if trade_df.empty:
            return self._empty_output()

        self._validate_input(trade_df)

        df = trade_df

        # Minute alignment (contract-defined)
        minute_ts = (df["ts"] // self.INTERVAL_MS) * self.INTERVAL_MS

        # Group by minute
        grouped = df.groupby(minute_ts, sort=True)

        bar_df = grouped.agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            volume=("volume", "sum"),
            trade_count=("price", "count"),
        )

        # Turnover: sum(price * volume)
        turnover = (df["price"] * df["volume"]).groupby(minute_ts).sum()
        bar_df["turnover"] = turnover

        # Finalize output
        bar_df = (
            bar_df.reset_index()
            .rename(columns={"index": "ts"})
            .rename_axis(None, axis=1)
            .sort_values("ts")
            .reset_index(drop=True)
        )

        return bar_df

    # ------------------------------------------------------------------
    # Validation & helpers
    # ------------------------------------------------------------------

    @classmethod
    def _validate_input(cls, df: pd.DataFrame) -> None:
        # Required columns
        missing = set(cls.REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # ts must be int
        if not pd.api.types.is_integer_dtype(df["ts"]):
            raise TypeError("Column 'ts' must be int (milliseconds)")

        # price / volume constraints
        if (df["price"] <= 0).any():
            raise ValueError("Column 'price' must be > 0")

        if (df["volume"] <= 0).any():
            raise ValueError("Column 'volume' must be > 0")

        # Must be sorted by ts ascending
        if not df["ts"].is_monotonic_increasing:
            raise ValueError("trade_df must be sorted by ts ascending")

    @staticmethod
    def _empty_output() -> pd.DataFrame:
        """
        Return an empty DataFrame that satisfies the output contract.
        """
        return pd.DataFrame(
            columns=[
                "ts",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "turnover",
                "trade_count",
            ]
        )
