# src/training/steps/rank_ic_series_report_step.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

from src import logs
from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext


class RankICSeriesReportStep(PipelineStep):
    """
    RankICSeriesReportStep (FINAL)

    Outputs:
    - rank_ic_series.csv
    - rank_ic_daily.png
    - rank_ic_rolling.png
    """

    stage = "training_report"

    def __init__(self, *, rolling_window: int = 20):
        self.rolling_window = rolling_window

    def run(self, ctx: TrainingContext) -> TrainingContext:
        if not getattr(ctx, "rank_ic_series", None):
            logs.warning("[RankICReport] no data, skipped")
            return ctx

        df = pd.DataFrame(ctx.rank_ic_series)
        df["eval_day"] = pd.to_datetime(df["eval_day"])
        df = df.sort_values("eval_day")

        out_dir = Path(ctx.model_dir) / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)

        # 1️⃣ CSV
        csv_path = out_dir / "rank_ic_series.csv"
        df.to_csv(csv_path, index=False)
        logs.info(f"[RankICReport] saved {csv_path}")

        # 2️⃣ Daily Rank IC
        plt.figure(figsize=(10, 4))
        plt.plot(df["eval_day"], df["rank_ic"], marker="o")
        plt.axhline(0.0, linestyle="--")
        plt.title("Daily Rank IC")
        plt.tight_layout()

        png_daily = out_dir / "rank_ic_daily.png"
        plt.savefig(png_daily)
        plt.close()
        logs.info(f"[RankICReport] saved {png_daily}")

        # 3️⃣ Rolling Rank IC
        plt.figure(figsize=(10, 4))
        rolling = df["rank_ic"].rolling(self.rolling_window).mean()
        plt.plot(df["eval_day"], rolling)
        plt.axhline(0.0, linestyle="--")
        plt.title(f"Rolling Rank IC ({self.rolling_window})")
        plt.tight_layout()

        png_rolling = out_dir / "rank_ic_rolling.png"
        plt.savefig(png_rolling)
        plt.close()
        logs.info(f"[RankICReport] saved {png_rolling}")

        return ctx
