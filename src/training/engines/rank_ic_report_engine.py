from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt


class RankICReportEngine:
    """
    RankICReportEngine（FINAL / FROZEN）

    Responsibility:
    - Persist Rank IC reports (CSV / PNG)
    """

    def write_csv(
        self,
        df: pd.DataFrame,
        out_dir: Path,
    ) -> Path:
        out_dir.mkdir(parents=True, exist_ok=True)
        path = out_dir / "rank_ic_series.csv"
        df.to_csv(path, index=False)
        return path

    def plot_daily(
        self,
        df: pd.DataFrame,
        out_dir: Path,
    ) -> Path:
        path = out_dir / "rank_ic_daily.png"

        plt.figure(figsize=(10, 4))
        plt.plot(df["eval_day"], df["rank_ic"], marker="o")
        plt.axhline(0.0, linestyle="--")
        plt.title("Daily Rank IC")
        plt.tight_layout()
        plt.savefig(path)
        plt.close()

        return path

    def plot_rolling(
        self,
        df: pd.DataFrame,
        rolling: pd.Series,
        window: int,
        out_dir: Path,
    ) -> Path:
        path = out_dir / "rank_ic_rolling.png"

        plt.figure(figsize=(10, 4))
        plt.plot(df["eval_day"], rolling)
        plt.axhline(0.0, linestyle="--")
        plt.title(f"Rolling Rank IC ({window})")
        plt.tight_layout()
        plt.savefig(path)
        plt.close()

        return path
