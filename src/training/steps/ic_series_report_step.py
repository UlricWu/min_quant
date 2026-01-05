# src/training/steps/ic_series_report_step.py
from __future__ import annotations

from pathlib import Path
from typing import Sequence, Dict, Any

import pandas as pd
import matplotlib.pyplot as plt

from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.utils.path import PathManager
from src import logs


class ICSeriesReportStep(PipelineStep):
    """
    ICSeriesReportStep（FINAL / FROZEN）

    Responsibility:
    - Render IC time series
    - Render rolling IC (mean / std)
    - Persist figures + raw csv

    Contract:
    - consumes ctx.ic_series
    - does NOT mutate ctx.model_state
    """

    def __init__(
        self,
        *,
        pm: PathManager,
        rolling_window: int = 20,
    ):
        self.pm = pm
        self.rolling_window = rolling_window

    def run(self, ctx: TrainingContext) -> TrainingContext:
        ic_series = getattr(ctx, "ic_series", None)

        if not ic_series:
            logs.warning("[ICReport] No IC series found, skip report")
            return ctx

        df = pd.DataFrame(ic_series)
        df["eval_day"] = pd.to_datetime(df["eval_day"])
        df = df.sort_values("eval_day").reset_index(drop=True)

        # --------------------------------------------------
        # Output directory
        # --------------------------------------------------
        out_dir = ctx.model_dir / "reports"
        out_dir.mkdir(parents=True, exist_ok=True)

        # --------------------------------------------------
        # Save raw IC series
        # --------------------------------------------------
        csv_path = out_dir / "ic_series.csv"
        df.to_csv(csv_path, index=False)
        logs.info(f"[ICReport] saved {csv_path}")

        # --------------------------------------------------
        # Plot 1: Daily IC
        # --------------------------------------------------
        fig1 = plt.figure(figsize=(10, 4))
        plt.plot(df["eval_day"], df["ic"], marker="o", linewidth=1)
        plt.axhline(0.0, linestyle="--", linewidth=1)
        plt.title("Daily IC")
        plt.xlabel("Date")
        plt.ylabel("IC")
        plt.grid(True)
        plt.tight_layout()

        fig1_path = out_dir / "ic_daily.png"
        fig1.savefig(fig1_path)
        plt.close(fig1)
        logs.info(f"[ICReport] saved {fig1_path}")

        # --------------------------------------------------
        # Plot 2: Rolling IC mean / std
        # --------------------------------------------------
        df["ic_mean"] = df["ic"].rolling(self.rolling_window).mean()
        df["ic_std"] = df["ic"].rolling(self.rolling_window).std()

        fig2 = plt.figure(figsize=(10, 4))
        plt.plot(df["eval_day"], df["ic_mean"], label="Rolling Mean")
        plt.fill_between(
            df["eval_day"],
            df["ic_mean"] - df["ic_std"],
            df["ic_mean"] + df["ic_std"],
            alpha=0.3,
            label="±1 Std",
        )
        plt.axhline(0.0, linestyle="--", linewidth=1)
        plt.title(f"Rolling IC ({self.rolling_window}D)")
        plt.xlabel("Date")
        plt.ylabel("IC")
        plt.legend()
        plt.grid(True)
        plt.tight_layout()

        fig2_path = out_dir / "ic_rolling.png"
        fig2.savefig(fig2_path)
        plt.close(fig2)
        logs.info(f"[ICReport] saved {fig2_path}")

        return ctx
