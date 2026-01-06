from __future__ import annotations

import numpy as np

from src import logs

"""
StrategyFactory (FINAL / FROZEN)

Contract:
- StrategyFactory consumes a resolved ModelArtifact (object), not dict.
- StrategyFactory does NOT construct train-time models.
- It builds an inference model from artifact, then constructs a Strategy.
- sklearn warnings about feature names are STRUCTURALLY eliminated.
"""

from typing import Dict, Type
import joblib

from src.pipeline.context import ModelArtifact
from src.backtest.strategy.base import InferenceModel, Strategy
from src.backtest.strategy.threshold import ThresholdStrategy
from src.backtest.strategy.feature_vectorizer import FeatureVectorizer


# =============================================================================
# Inference Model (execution-domain, sklearn-adapted)
# =============================================================================
class SklearnInferenceModel(InferenceModel):
    """
    Inference wrapper for sklearn models.

    FROZEN rules:
    - Feature alignment is handled by FeatureVectorizer
    - sklearn ONLY receives ndarray (no DataFrame)
    - NaN policy is inference-only and explicit
    """

    def __init__(self, model, *, feature_names: list[str]):
        self._model = model
        self._vectorizer = FeatureVectorizer(feature_names)

    def predict(self, features_by_symbol: Dict[str, dict]) -> Dict[str, float]:
        # --------------------------------------------------
        # 1. Vectorize (symbol -> aligned matrix)
        # --------------------------------------------------
        X_df, symbols = self._vectorizer.transform(features_by_symbol)

        # --------------------------------------------------
        # 2. NaN handling (FROZEN inference policy)
        # --------------------------------------------------
        # Training-time contract: model never saw NaN
        # Inference-time policy: NaN -> 0.0
        X_df = X_df.replace([np.inf, -np.inf], np.nan)
        X_df = X_df.fillna(0.0)
        #
        # mask = np.isfinite(X_df.values).all(axis=1)
        # X_df = X_df[mask]
        # symbols = [s for i, s in enumerate(symbols) if mask[i]]

        # Safety invariant
        assert X_df.shape[1] == len(self._vectorizer.feature_names)

        # --------------------------------------------------
        # 3. 🔒 sklearn call (ndarray ONLY)
        # --------------------------------------------------
        preds = self._model.predict(X_df.values)
        if np.random.rand() < 0.001:
            logs.info(
                f"[Inference] score stats: "
                f"min={preds.min():.4f}, "
                f"mean={preds.mean():.4f}, "
                f"max={preds.max():.4f}"
            )

        return dict(zip(symbols, preds))


# =============================================================================
# Inference Model Factory (STATIC / FROZEN)
# =============================================================================
class InferenceModelFactory:
    """
    Build execution-domain inference models from artifacts.
    """

    @staticmethod
    def build(artifact: ModelArtifact) -> InferenceModel:
        if not isinstance(artifact, ModelArtifact):
            raise TypeError(
                "[InferenceModelFactory] artifact must be ModelArtifact, "
                f"got {type(artifact)}"
            )

        model = joblib.load(artifact.path)

        return SklearnInferenceModel(
            model=model,
            feature_names=artifact.feature_names,
        )


# =============================================================================
# Strategy Factory (STATIC / FROZEN)
# =============================================================================
class StrategyFactory:
    """
    Static Strategy Factory (FINAL).

    Registry holds Strategy classes ONLY.
    Models are injected via artifacts.
    """

    _REGISTRY: Dict[str, Type[Strategy]] = {
        "threshold": ThresholdStrategy,
        # Future strategies MUST be added explicitly here.
    }

    @classmethod
    def build(cls, cfg: Dict) -> Strategy:
        if "type" not in cfg:
            raise KeyError("[StrategyFactory] missing 'type' in strategy config")

        typ = cfg["type"]
        if typ not in cls._REGISTRY:
            raise ValueError(f"[StrategyFactory] unknown strategy type: {typ}")

        model_cfg = cfg.get("model", {})
        params = cfg.get("params", {})

        # --------------------------------------------------
        # 🔒 FROZEN CONTRACT: artifact must be resolved
        # --------------------------------------------------
        if "artifact" not in model_cfg:
            raise KeyError(
                "[StrategyFactory] model.artifact (ModelArtifact) is required"
            )

        artifact = model_cfg["artifact"]
        if not isinstance(artifact, ModelArtifact):
            raise TypeError(
                "[StrategyFactory] model.artifact must be ModelArtifact, "
                f"got {type(artifact)}"
            )

        # --------------------------------------------------
        # Build inference model
        # --------------------------------------------------
        model = InferenceModelFactory.build(artifact)

        # --------------------------------------------------
        # Build strategy (model injected)
        # --------------------------------------------------
        strategy_cls = cls._REGISTRY[typ]
        return strategy_cls(model=model, **params)
