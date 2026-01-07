from __future__ import annotations

import pandas as pd


class RankICSeriesEngine:
    """
    RankICSeriesEngine（FINAL / FROZEN）

    Responsibility:
    - Build time-series dataframe
    - Compute rolling statistics
    """

    def build_series(
        self,
        records: list[dict],
    ) -> pd.DataFrame:
        df = pd.DataFrame(records)
        df["eval_day"] = pd.to_datetime(df["eval_day"])
        return df.sort_values("eval_day")

    def rolling_mean(
        self,
        df: pd.DataFrame,
        *,
        window: int,
    ) -> pd.Series:
        return df["rank_ic"].rolling(window).mean()
