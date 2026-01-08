# src/training/steps/model_train_step.py
from __future__ import annotations

from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.training.engines.model.sgd_regressor_train_engine import (
    SklearnSGDRegressorTrainEngine,
)


class ModelTrainStep(PipelineStep):
    """
    ModelTrainStep（ONLINE / FINAL）

    Contract:
    - consumes ctx.train_X / ctx.train_y
    - consumes ctx.model_state (may be None)
    - produces ctx.model_state
    """

    def __init__(self, cfg):
        super().__init__()
        self.engine = SklearnSGDRegressorTrainEngine(cfg)

    def run(self, ctx: TrainingContext) -> TrainingContext:
        state = self.engine.train(
            X=ctx.train_X,
            y=ctx.train_y,
            prev_state=ctx.model_state,
            asof_day=ctx.update_day,
        )

        ctx.model_state = state
        return ctx
