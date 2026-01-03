from __future__ import annotations

"""
{#!filepath: src/backtest/strategy/ml/rf_model.py}

RandomForestModel (FINAL / FROZEN)

Sklearn-based RandomForest inference wrapper.

Responsibilities:
- Load a trained sklearn RandomForest model
- Convert feature dict -> numpy matrix
- Output per-symbol probability or score

This class performs inference ONLY.
"""

from typing import Dict, Any, List
import numpy as np
import joblib

from src.backtest.strategy.base import Model


class RandomForestModel(Model):
    def __init__(
        self,
        *,
        model_path: str,
        feature_order: List[str],
        proba_index: int = 1,
    ):
        """
        Parameters:
        - model_path: path to trained sklearn model (.joblib)
        - feature_order: fixed feature order expected by the model
        - proba_index: which class probability to use
        """
        if not model_path.endswith(".joblib"):
            raise ValueError(
                "RandomForestModel expects a .joblib artifact"
            )

        self.model = joblib.load(model_path)
        self.feature_order = feature_order
        self.proba_index = proba_index

    def predict(
        self,
        features_by_symbol: Dict[str, Dict[str, Any]],
    ) -> Dict[str, float]:
        symbols = list(features_by_symbol.keys())

        if not symbols:
            return {}

        X = np.array([
            [features_by_symbol[s].get(f, 0.0) for f in self.feature_order]
            for s in symbols
        ])

        # sklearn RF
        if hasattr(self.model, "predict_proba"):
            probs = self.model.predict_proba(X)[:, self.proba_index]
            return dict(zip(symbols, probs))

        # fallback: regression-style
        preds = self.model.predict(X)
        return dict(zip(symbols, preds))
