from __future__ import annotations

from pathlib import Path
from typing import List, Tuple, Optional

import numpy as np
import pandas as pd

from src.utils.path import PathManager


class DatasetBuildEngine:
    """
    DatasetBuildEngine（FINAL / FROZEN）

    Responsibility:
    - Build day-level training / evaluation datasets
    - Own ALL dataset construction semantics:
        - file discovery
        - feature / label alignment
        - numeric sanitization (inf / NaN)
        - row filtering (drop_na)
        - concatenation across symbols/files

    Contract (FROZEN):
    - Step MUST NOT perform any pandas / IO / data cleaning logic
    - Engine guarantees:
        - X / y index-aligned
        - No inf values
        - drop_na behavior strictly follows cfg
    """

    def __init__(self, pm: PathManager):
        self.pm = pm

    # ======================================================================
    # Public API
    # ======================================================================
    def build(
            self,
            *,
            update_day: str,
            eval_day: Optional[str],
            feature_columns: Optional[list[str]],
            label_column: str,
            drop_na: bool,
            evaluation_enabled: bool,
    ) -> Tuple[pd.DataFrame, pd.Series, Optional[pd.DataFrame], Optional[pd.Series]]:
        """
        Build train / eval datasets.

        Returns:
            train_X, train_y, eval_X, eval_y
        """

        train_X, train_y = self._build_one_day(
            date=update_day,
            feature_columns=feature_columns,
            label_column=label_column,
            drop_na=drop_na,
        )

        if evaluation_enabled and eval_day is not None:
            eval_X, eval_y = self._build_one_day(
                date=eval_day,
                feature_columns=feature_columns,
                label_column=label_column,
                drop_na=drop_na,
            )
        else:
            eval_X, eval_y = None, None

        return train_X, train_y, eval_X, eval_y

    # ======================================================================
    # Internal
    # ======================================================================
    def _build_one_day(
            self,
            *,
            date: str,
            feature_columns: Optional[list[str]],
            label_column: str,
            drop_na: bool,
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Build dataset for a single physical day.

        Invariants:
        - Feature / label must be suffix-aligned
        - Numeric sanitization is ALWAYS applied
        """

        feat_dir = self.pm.feature_dir(date)
        lab_dir = self.pm.label_dir(date)

        frames_X: List[pd.DataFrame] = []
        frames_y: List[pd.Series] = []

        for feat_file in sorted(feat_dir.glob("feature.*.parquet")):
            suffix = feat_file.name[len("feature."):]
            lab_file = lab_dir / f"label.{suffix}"

            if not lab_file.exists():
                # Explicitly skip unmatched feature files
                continue

            X, y = self._load_and_align_one_pair(
                feat_file=feat_file,
                lab_file=lab_file,
                feature_columns=feature_columns,
                label_column=label_column,
                drop_na=drop_na,
            )

            if len(X) == 0:
                continue

            frames_X.append(X)
            frames_y.append(y)

        if not frames_X:
            raise RuntimeError(f"No valid dataset built for date={date}")

        return (
            pd.concat(frames_X, ignore_index=True),
            pd.concat(frames_y, ignore_index=True),
        )

    # ------------------------------------------------------------------
    # Pair-level logic (atomic & testable)
    # ------------------------------------------------------------------
    def _load_and_align_one_pair(
            self,
            *,
            feat_file: Path,
            lab_file: Path,
            feature_columns: Optional[list[str]],
            label_column: str,
            drop_na: bool,
    ) -> Tuple[pd.DataFrame, pd.Series]:
        """
        Load one (feature, label) file pair and return sanitized X / y.
        """

        feat_df = pd.read_parquet(feat_file)
        lab_df = pd.read_parquet(lab_file)

        # ------------------------------
        # Column selection
        # ------------------------------
        if feature_columns is not None:
            X = feat_df[feature_columns].copy()
        else:
            X = feat_df.copy()

        y = lab_df[label_column].copy()

        # ==============================================================
        # Numeric sanitization 数值合法性保证(唯一合法位置)
        # ==============================================================
        # 1) inf → NaN
        X.replace([np.inf, -np.inf], np.nan, inplace=True)
        y.replace([np.inf, -np.inf], np.nan, inplace=True)

        # 2) drop invalid rows
        if drop_na:
            mask = X.notna().all(axis=1) & y.notna()
            X = X.loc[mask]
            y = y.loc[mask]

        return X, y
