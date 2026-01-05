# src/training/steps/model_evaluate_step.py
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.utils.path import PathManager


class ICEvaluateStep(PipelineStep):
    """
    Rank IC evaluation (walk-forward)
    """

    def __init__(self, pm: PathManager):
        self.pm = pm

    def run(self, ctx: TrainingContext) -> TrainingContext:
        if ctx.eval_day is None:
            return ctx

        # date = ctx.eval_day.strftime("%Y-%m-%d")
        date = ctx.eval_day

        feat_path = self.pm.feature_dir(date) / "feature.sh_trade.parquet"
        lab_path = self.pm.label_dir(date) / "label.sh_trade.parquet"

        X = pd.read_parquet(feat_path)[ctx.cfg.dataset.feature_columns]
        y = pd.read_parquet(lab_path)[ctx.cfg.dataset.label_column]

        X.replace([np.inf, -np.inf], np.nan, inplace=True)
        y.replace([np.inf, -np.inf], np.nan, inplace=True)

        mask = X.notna().all(axis=1) & y.notna()
        X = X.loc[mask]
        y = y.loc[mask]

        preds = ctx.model_state.model.predict(X.values)

        ic, _ = spearmanr(preds, y.values)

        ctx.metrics[f"ic@{date}"] = ic
        return ctx
