# src/training/engines/model/sgd_regressor_train_engine.py
from __future__ import annotations

import pandas as pd
from sklearn.linear_model import SGDRegressor

from src.training.context import ModelState


class SklearnSGDRegressorTrainEngine:
    """
    SGDRegressor Online Train Engine（FINAL）
    """

    def __init__(self, cfg):
        self.cfg = cfg

    def train(
        self,
        *,
        X: pd.DataFrame,
        y: pd.Series,
        prev_state: ModelState | None,
        asof_day: pd.Timestamp,
    ) -> ModelState:
        if prev_state is None:
            model = SGDRegressor(**self.cfg.model_params)
            model.partial_fit(X.values, y.values)
        else:
            model = prev_state.model
            model.partial_fit(X.values, y.values)

        return ModelState(model=model, asof_day=asof_day)
