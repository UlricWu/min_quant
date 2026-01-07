# src/training/engines/ic_evaluate_engine.py
from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import spearmanr


class ICEvaluateEngine:
    """
    ICEvaluateEngine（FINAL / FROZEN）

    Responsibility:
    - Perform rank-based IC evaluation
    - Own ALL statistical semantics

    Contract:
    - Input X / y MUST already be:
        - index-aligned
        - numeric-clean (no inf)
    - Engine guarantees:
        - deterministic IC definition
        - NaN-safe handling
    """

    def evaluate(
        self,
        *,
        model,
        X: pd.DataFrame,
        y: pd.Series,
    ) -> tuple[np.ndarray, float]:
        """
        Returns:
            preds, ic_value
        """

        preds = model.predict(X.values)

        ic = self._compute_rank_ic(preds, y.values)

        return preds, ic

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    @staticmethod
    def _compute_rank_ic(
        preds: np.ndarray,
        y_true: np.ndarray,
    ) -> float:
        """
        Spearman Rank IC (frozen definition)
        """

        if len(preds) == 0:
            return float("nan")

        ic, _ = spearmanr(preds, y_true)

        return float(ic)
