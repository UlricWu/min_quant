from typing import Callable, Dict, Tuple

from src.training.engines.model_train_engine import ModelTrainEngine
from src.training.engines.model.sklearn_sgd_classifier_train_engine import (
    SklearnSGDClassifierTrainEngine,
)
from src.training.engines.model.sgd_regressor_train_engine import (
    SklearnSGDRegressorTrainEngine,
)
from src.config.training_config import TrainingConfig
from src.pipeline.model_artifact import ModelSpec

_ENGINE_REGISTRY: Dict[
    Tuple[str, str, str],
    Callable[[TrainingConfig], ModelTrainEngine],
] = {
    ("sgd", "regression", "v1"): lambda cfg: SklearnSGDRegressorTrainEngine(cfg),
    ("sgd", "classification", "v1"): lambda cfg: SklearnSGDClassifierTrainEngine(cfg),
}


def resolve_model_train_engine(
        *, spec: ModelSpec, cfg: TrainingConfig
) -> ModelTrainEngine:
    key = (spec.family, spec.task, spec.version)

    if key not in _ENGINE_REGISTRY:
        available = ", ".join(str(k) for k in _ENGINE_REGISTRY)
        raise ValueError(
            f"No ModelTrainEngine for {key}. Available: {available}"
        )

    return _ENGINE_REGISTRY[key](cfg)
