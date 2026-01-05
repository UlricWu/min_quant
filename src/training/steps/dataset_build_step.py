from __future__ import annotations

from pathlib import Path
from typing import List, Tuple

import pandas as pd

from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.utils.path import PathManager

import numpy as np
class DatasetBuildStep(PipelineStep):
    """
    DatasetBuildStep（FINAL）

    Semantics:
    - Build train set from ctx.today
    - Build eval set from ctx.next_day
    - 1-day minibatch only
    """

    def __init__(self):
        self.pm = PathManager()

    def run(self, ctx: TrainingContext) -> TrainingContext:
        cfg = ctx.cfg.dataset

        # --------------------------------------------------
        # Train (today)
        # --------------------------------------------------
        train_X, train_y = self._load_one_day(
            date=ctx.update_day,
            feature_columns=cfg.feature_columns,
            label_column=cfg.label_column,
            drop_na=cfg.drop_na,
        )

        # --------------------------------------------------
        # Eval (next_day, optional)
        # --------------------------------------------------
        if ctx.eval_day is not None and ctx.cfg.evaluation_enabled:
            eval_X, eval_y = self._load_one_day(
                date=ctx.eval_day,
                feature_columns=cfg.feature_columns,
                label_column=cfg.label_column,
                drop_na=cfg.drop_na,
            )
        else:
            eval_X, eval_y = None, None

        ctx.train_X = train_X
        ctx.train_y = train_y
        ctx.eval_X = eval_X
        ctx.eval_y = eval_y

        return ctx

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _load_one_day(
        self,
        *,
        date: str,
        feature_columns: list[str] | None,
        label_column: str,
        drop_na: bool,
    ) -> Tuple[pd.DataFrame, pd.Series]:
        feat_dir = self.pm.feature_dir(date)
        lab_dir = self.pm.label_dir(date)

        frames_X: List[pd.DataFrame] = []
        frames_y: List[pd.Series] = []

        for feat_file in sorted(feat_dir.glob("feature.*.parquet")):
            suffix = feat_file.name[len("feature.") :]
            lab_file = lab_dir / f"label.{suffix}"

            if not lab_file.exists():
                continue

            feat_df = pd.read_parquet(feat_file)
            lab_df = pd.read_parquet(lab_file)

            # ------------------------------
            # Select columns
            # ------------------------------
            if feature_columns is not None:
                X = feat_df[feature_columns].copy()
            else:
                X = feat_df.copy()

            y = lab_df[label_column].copy()

            # ==================================================
            # ⭐ 核心修复：数值合法性保证（唯一正确位置）
            # ==================================================
            # 1) inf → NaN
            X.replace([np.inf, -np.inf], np.nan, inplace=True)
            y.replace([np.inf, -np.inf], np.nan, inplace=True)

            # 2) drop invalid rows
            if drop_na:
                mask = X.notna().all(axis=1) & y.notna()
                X = X.loc[mask]
                y = y.loc[mask]

            frames_X.append(X)
            frames_y.append(y)

        if not frames_X:
            raise RuntimeError(f"No valid data found for date={date}")

        return (
            pd.concat(frames_X, ignore_index=True),
            pd.concat(frames_y, ignore_index=True),
        )