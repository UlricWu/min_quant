# src/training/steps/model_report_step.py
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

from src import logs
from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.training.engines.model_report_engine import ModelReportEngine


class ModelReportStep(PipelineStep):
    """
    ModelReportStep（ONLINE / FINAL / FROZEN）

    Responsibility:
    - Walk-forward evaluation (Model(t) on eval_day)
    - Persist metrics
    - Does NOT modify model state
    """

    stage = "model_report"

    def __init__(self, *, inst,engine: ModelReportEngine):
        super().__init__(inst)
        self.engine = engine

    def run(self, ctx: TrainingContext) -> TrainingContext:
        # --------------------------------------------------
        # Skip conditions (NOT errors)
        # --------------------------------------------------
        if ctx.eval_X is None or ctx.eval_y is None:
            logs.info(
                f"[ModelReportStep] skip evaluation "
                f"(no eval data) update_day={ctx.update_day_str}"
            )
            return ctx

        if ctx.model_state is None:
            logs.info(
                f"[ModelReportStep] skip evaluation "
                f"(model_state is None) update_day={ctx.update_day_str}"
            )
            return ctx

        # --------------------------------------------------
        # Evaluate (walk-forward)
        # --------------------------------------------------
        logs.info(
            f"[ModelReportStep] evaluate "
            f"model_asof={ctx.model_state.asof_day:%Y-%m-%d} "
            f"on eval_day={ctx.eval_day_str}"
        )

        metrics = self.engine.evaluate(
            model=ctx.model_state.model,
            X=ctx.eval_X,
            y=ctx.eval_y,
        )

        # --------------------------------------------------
        # Enrich & persist metrics
        # --------------------------------------------------
        record = {
            "update_day": ctx.update_day_str,
            "eval_day": ctx.eval_day_str,
            "model_asof_day": ctx.model_state.asof_day.strftime("%Y-%m-%d"),
            "metrics": metrics,
            "created_at": datetime.utcnow().isoformat(),
        }

        ctx.metrics.update(metrics)

        out_dir = Path(ctx.model_dir) / "metrics"
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"{ctx.update_day_str}.json"
        out_file.write_text(
            json.dumps(record, indent=2, ensure_ascii=False)
        )

        logs.info(f"[ModelReportStep] metrics saved: {out_file}")

        return ctx
