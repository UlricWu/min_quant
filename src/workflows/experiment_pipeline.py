# src/workflows/experiment_pipeline.py
from __future__ import annotations

from src.pipeline.context import ModelArtifact


class ExperimentPipeline:
    """
    ExperimentPipeline (FINAL / FROZEN)

    Semantics:
    - Owns execution order: training -> backtest
    - Responsible for artifact handoff
    - NOT reusable for training-only or backtest-only
    """

    def __init__(self, *, training_pipeline, backtest_pipeline):
        self.training_pipeline = training_pipeline
        self.backtest_pipeline = backtest_pipeline

    def run(self, *, run_id: str):
        # -----------------------------
        # 1) Training
        # -----------------------------
        train_ctx = self.training_pipeline.run(run_id=run_id)

        artifact: ModelArtifact | None = getattr(
            train_ctx, "model_artifact", None
        )
        if artifact is None:
            raise RuntimeError(
                "[ExperimentPipeline] Training did not produce ModelArtifact"
            )

        # -----------------------------
        # 2) Inject artifact (ONLY PLACE)
        # -----------------------------
        backtest_cfg = self.backtest_pipeline.cfg
        backtest_cfg.strategy.setdefault("model", {})
        backtest_cfg.strategy["model"]["artifact"] = artifact

        # -----------------------------
        # 3) Backtest
        # -----------------------------
        return self.backtest_pipeline.run(run_id=run_id)
