# src/training/engines/dataset_build_engine.py
from __future__ import annotations

from pathlib import Path
from typing import Tuple, List

import pandas as pd

from src.training.context import TrainingContext
from src.training.steps.dataset_resolve_step import DatasetDeclaration
from src.utils.path import PathManager


class DatasetBuildEngine:
    """
    DatasetBuildEngine（FINAL / FROZEN）

    Responsibility:
    - Load ONE trading day dataset from disk
    - Build (train_X, train_y) and (eval_X, eval_y)
    - No knowledge of exchanges / symbols / models

    Assumptions:
    - feature/<date>/ contains multiple parquet files
    - label/<date>/ contains corresponding parquet files
    """

    def __init__(self) -> None:
        self.pm = PathManager()

    # ------------------------------------------------------------------
    # Entry
    # ------------------------------------------------------------------
    def build(
        self,
        *,
        ctx: TrainingContext,
    ) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame | None, pd.Series | None]:
        dataset: DatasetDeclaration = ctx.dataset

        update_day = ctx.update_day_str
        eval_day = ctx.eval_day_str

        # --------------------------------------------------
        # Build train dataset (update_day)
        # --------------------------------------------------
        train_X, train_y = self._load_one_day(
            date=update_day,
            dataset=dataset,
        )

        # --------------------------------------------------
        # Build eval dataset (eval_day, optional)
        # --------------------------------------------------
        if eval_day is not None:
            eval_X, eval_y = self._load_one_day(
                date=eval_day,
                dataset=dataset,
            )
        else:
            eval_X, eval_y = None, None

        return train_X, train_y, eval_X, eval_y

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _load_one_day(
        self,
        *,
        date: str,
        dataset: DatasetDeclaration,
    ) -> Tuple[pd.DataFrame, pd.Series]:
        feat_dir = self.pm.feature_dir(date)
        lab_dir = self.pm.label_dir(date)

        frames_X: List[pd.DataFrame] = []
        frames_y: List[pd.Series] = []

        for feat_file in sorted(feat_dir.glob("feature.*.parquet")):
            lab_file = lab_dir / feat_file.name.replace("feature.", "label.")

            if not lab_file.exists():
                continue

            feat_df = pd.read_parquet(feat_file)
            lab_df = pd.read_parquet(lab_file)

            # ------------------------------
            # Select columns
            # ------------------------------
            if dataset.feature_columns is not None:
                X = feat_df[dataset.feature_columns]
            else:
                X = feat_df.drop(columns=[dataset.label_column], errors="ignore")

            y = lab_df[dataset.label_column]

            if dataset.drop_na:
                mask = X.notna().all(axis=1) & y.notna()
                X = X[mask]
                y = y[mask]

            frames_X.append(X)
            frames_y.append(y)

        if not frames_X:
            raise RuntimeError(f"No data found for date={date}")

        return pd.concat(frames_X), pd.concat(frames_y)
