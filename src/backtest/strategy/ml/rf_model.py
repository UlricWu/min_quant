#!filepath: src/backtest/strategy/ml/rf_model.py
from __future__ import annotations

"""
RandomForestInferenceModel (FINAL / FROZEN)

Sklearn-based RandomForest inference wrapper.

Responsibilities:
- Load a trained sklearn RandomForest artifact
- Convert feature dict -> numpy matrix
- Output per-symbol probability or score

This class performs inference ONLY.
"""

from typing import Dict, Any, List
import numpy as np
import joblib

from src.backtest.strategy.ml.inference_model import InferenceModel


class RandomForestInferenceModel(InferenceModel):
    """
        Works for:
        - RF
        - XGB
        - LGBM
        - LogisticRegression
        - SGDClassifier
        """

    def __init__(
        self,
        *,
        model_path: str,
        feature_order: List[str],
        proba_index: int = 1,
    ):
        if not model_path.endswith(".joblib"):
            raise ValueError(
                "RandomForestInferenceModel expects a .joblib artifact"
            )

        artifact = joblib.load(model_path)

        # Artifact contract (training-defined, inference-consumed)
        self.model = artifact["model"]
        self.feature_order = artifact["feature_order"]
        self.proba_index = proba_index

    def predict(
        self,
        *,
        features_by_symbol: Dict[str, Dict[str, Any]],
    ) -> Dict[str, float]:
        symbols = list(features_by_symbol.keys())
        if not symbols:
            return {}

        X = np.array(
            [
                [features_by_symbol[s].get(f, 0.0) for f in self.feature_order]
                for s in symbols
            ],
            dtype=np.float32,
        )

        # Classification-style
        if hasattr(self.model, "predict_proba"):
            probs = self.model.predict_proba(X)[:, self.proba_index]
            return dict(zip(symbols, probs))

        # Regression-style fallback
        preds = self.model.predict(X)
        return dict(zip(symbols, preds))
