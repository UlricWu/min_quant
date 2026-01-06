from __future__ import annotations

from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.training.engines.rank_ic_evaluate_engine import RankICEvaluateEngine
from src import logs


class RankICStep(PipelineStep):
    """
    RankICStep（FINAL / FROZEN）

    Responsibility:
    - Compute Rank IC for current eval_day
    - Append structured record to ctx.rank_ic_series
    """

    stage = "training_report"

    def __init__(self, engine: RankICEvaluateEngine) -> None:
        self.engine = engine

    def run(self, ctx: TrainingContext) -> TrainingContext:
        if ctx.eval_pred is None or ctx.eval_y is None:
            logs.warning(
                f"[RankIC] eval={ctx.eval_day} SKIPPED (no eval data)"
            )
            return ctx

        if not hasattr(ctx, "rank_ic_series"):
            ctx.rank_ic_series = []

        rank_ic = self.engine.evaluate(
            preds=ctx.eval_pred,
            y_true=ctx.eval_y.values,
        )

        record = {
            "eval_day": ctx.eval_day,
            "rank_ic": rank_ic,
        }
        ctx.rank_ic_series.append(record)

        logs.info(
            f"[RankIC] eval={ctx.eval_day} rank_ic={rank_ic:.6f}"
        )

        return ctx
