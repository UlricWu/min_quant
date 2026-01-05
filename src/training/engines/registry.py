from typing import Type

from src.training.engines.model_train_engine import ModelTrainEngine
from src.training.engines.model.sklearn_sgd_classifier_train_engine import (
    SklearnSGDClassifierTrainEngine,
)

from src.training.engines.model.sgd_regressor_train_engine import SklearnSGDRegressorTrainEngine

_ENGINE_REGISTRY: dict[tuple[str, str], Type[ModelTrainEngine]] = {
    ("sgd", "classifier_v1"): SklearnSGDClassifierTrainEngine,
    ("sgd", "regressor_v1"): SklearnSGDRegressorTrainEngine
}


def resolve_model_train_engine(*, model_name: str, model_version: str) -> Type[ModelTrainEngine]:
    key = (model_name, model_version)
    if key not in _ENGINE_REGISTRY:
        raise ValueError(f"No ModelTrainEngine registered for {key}")
    return _ENGINE_REGISTRY[key]
