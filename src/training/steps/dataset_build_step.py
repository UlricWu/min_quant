from __future__ import annotations

from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.training.engines.dataset_build_engine import DatasetBuildEngine
from src.utils.path import PathManager


class DatasetBuildStep(PipelineStep):
    """
    DatasetBuildStep（FINAL / FROZEN）

    Responsibility:
    - Orchestrate dataset construction semantics
    - Delegate ALL logic to DatasetBuildEngine

    Forbidden:
    - pandas operations
    - file IO
    - data cleaning logic
    """

    def __init__(self, pm: PathManager, inst):
        super().__init__(inst)
        self.engine = DatasetBuildEngine(pm)

    def run(self, ctx: TrainingContext) -> TrainingContext:
        cfg = ctx.cfg.dataset

        train_X, train_y, eval_X, eval_y = self.engine.build(
            update_day=ctx.update_day,
            eval_day=ctx.eval_day,
            feature_columns=cfg.feature_columns,
            label_column=cfg.label_column,
            drop_na=cfg.drop_na,
            evaluation_enabled=ctx.cfg.evaluation_enabled,
        )

        ctx.train_X = train_X
        ctx.train_y = train_y
        ctx.eval_X = eval_X
        ctx.eval_y = eval_y

        return ctx
