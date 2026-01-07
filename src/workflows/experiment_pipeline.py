# src/workflows/experiment_pipeline.py
from __future__ import annotations

from src.pipeline.model_artifact import (
    ModelArtifact,
    promote_model_artifact,
)
from src import logs


class ExperimentPipeline:
    """
    ExperimentPipeline（FINAL / FROZEN）

    Semantics:
    - Owns train → promote → backtest lifecycle
    - Is the ONLY authority that can publish models
    """

    def __init__(self, *, training_pipeline, backtest_pipeline):
        self.training_pipeline = training_pipeline
        self.backtest_pipeline = backtest_pipeline

    def run(self, *, run_id: str):
        # -----------------------------
        # 1) Training (run-scoped)
        # -----------------------------
        logs.info(f"[Experiment] TRAIN run_id={run_id}")
        train_ctx = self.training_pipeline.run(run_id=run_id)

        artifact: ModelArtifact | None = getattr(
            train_ctx, "model_artifact", None
        )
        if artifact is None:
            raise RuntimeError(
                "[ExperimentPipeline] Training did not produce ModelArtifact"
            )

        # -----------------------------
        # 2) PROMOTE (ONLY HERE)
        # -----------------------------
        logs.info(
            f"[Experiment] PROMOTE model={train_ctx.cfg.name}"
        )
        promote_model_artifact(
            artifact=artifact,
            model_name=train_ctx.cfg.name,
        )

        # -----------------------------
        # 3) Backtest (consume published)
        # -----------------------------
        logs.info("[Experiment] BACKTEST (published model)")
        return self.backtest_pipeline.run(run_id=run_id)
