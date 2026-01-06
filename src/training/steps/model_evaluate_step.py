# src/training/steps/model_evaluate_step.py
from __future__ import annotations

from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.training.engines.ic_evaluate_engine import ICEvaluateEngine


class ICEvaluateStep(PipelineStep):
    """
    ICEvaluateStep（FINAL / FROZEN）

    Responsibility:
    - Orchestrate IC evaluation
    - Delegate ALL computation to ICEvaluateEngine
    """

    def __init__(self, engine: ICEvaluateEngine) -> None:
        self.engine = engine

    def run(self, ctx: TrainingContext) -> TrainingContext:
        if ctx.eval_X is None or ctx.eval_y is None:
            return ctx

        preds, ic = self.engine.evaluate(
            model=ctx.model_state.model,
            X=ctx.eval_X,
            y=ctx.eval_y,
        )

        date = ctx.eval_day

        # Write back (artifact contract)
        ctx.eval_pred = preds
        ctx.metrics[f"ic@{date}"] = ic

        return ctx
