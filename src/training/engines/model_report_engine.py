# src/training/engines/model_report_engine.py
from __future__ import annotations

from typing import Any, Dict

import numpy as np
import pandas as pd
from sklearn.metrics import (
    roc_auc_score,
    f1_score,
    accuracy_score,
)

from src import logs


class ModelReportEngine:
    """
    ModelReportEngine（FINAL / FROZEN）

    Responsibility:
    - Evaluate model on walk-forward dataset
    - Return pure metrics dict (no side effects)
    """

    # ------------------------------------------------------------------
    # Public API (FROZEN)
    # ------------------------------------------------------------------
    def evaluate(
        self,
        *,
        model: Any,
        X: pd.DataFrame,
        y: pd.Series,
    ) -> Dict[str, float]:
        """
        Evaluate classification model.

        Contract:
        - model must support predict()
        - predict_proba() is optional but recommended
        """

        if len(X) == 0:
            raise ValueError("[ModelReportEngine] empty eval dataset")

        # --------------------------------------------------
        # Predictions
        # --------------------------------------------------
        y_pred = model.predict(X)

        metrics: Dict[str, float] = {
            "accuracy": accuracy_score(y, y_pred),
            "f1": f1_score(y, y_pred),
        }

        # --------------------------------------------------
        # Probabilistic metrics (if available)
        # --------------------------------------------------
        if hasattr(model, "predict_proba"):
            try:
                y_prob = model.predict_proba(X)[:, 1]
                metrics["auc"] = roc_auc_score(y, y_prob)
            except Exception as e:
                logs.info(
                    f"[ModelReportEngine] predict_proba failed, skip AUC: {e}"
                )

        return metrics
