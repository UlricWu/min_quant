from __future__ import annotations

import numpy as np
from typing import Dict, Type
from pathlib import Path
import joblib

from src import logs
from src.pipeline.model_artifact import ModelArtifact
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
        # 1. Vectorize
        X_df, symbols = self._vectorizer.transform(features_by_symbol)

        # 2. NaN handling (inference policy)
        X_df = X_df.replace([np.inf, -np.inf], np.nan)
        X_df = X_df.fillna(0.0)

        assert X_df.shape[1] == len(self._vectorizer.feature_names)

        # 3. sklearn inference (ndarray ONLY)
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

    FINAL / FROZEN
    """

    @staticmethod
    def build(artifact: ModelArtifact) -> InferenceModel:
        if not isinstance(artifact, ModelArtifact):
            raise TypeError(
                "[InferenceModelFactory] artifact must be ModelArtifact, "
                f"got {type(artifact)}"
            )

        # 🔒 FROZEN: artifact.path is ROOT DIR
        model_path = Path(artifact.path) / "model.joblib"
        if not model_path.exists():
            raise FileNotFoundError(
                f"[InferenceModelFactory] model file not found: {model_path}"
            )

        model = joblib.load(model_path)

        return SklearnInferenceModel(
            model=model,
            feature_names=artifact.feature_names,
        )


# =============================================================================
# Strategy Factory (STATIC / FROZEN)
# =============================================================================
class StrategyFactory:
    """
    Static Strategy Factory (FINAL / FROZEN)

    - Registry holds Strategy classes ONLY
    - Models are injected via resolved ModelArtifact
    """

    _REGISTRY: Dict[str, Type[Strategy]] = {
        "threshold": ThresholdStrategy,
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
        # 🔒 FROZEN CONTRACT
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
        # Build strategy
        # --------------------------------------------------
        strategy_cls = cls._REGISTRY[typ]
        return strategy_cls(model=model, **params)
