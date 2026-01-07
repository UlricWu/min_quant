# src/training/engines/model/sgd_classifier_train_engine.py
from __future__ import annotations

import pandas as pd
from sklearn.linear_model import SGDClassifier

from src.training.engines.model_train_engine import ModelTrainEngine
class SGDClassifierTrainEngine(ModelTrainEngine):
    def train(
        self,
        *,
        X: pd.DataFrame,
        y: pd.Series,
        prev_model=None,
    ):
        if prev_model is None:
            model = SGDClassifier(**self.cfg.model_params)
            model.partial_fit(X, y, classes=self.cfg.classes)
            return model

        # Incremental update
        prev_model.partial_fit(X, y)
        return prev_model
