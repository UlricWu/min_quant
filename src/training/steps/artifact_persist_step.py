from __future__ import annotations

from pathlib import Path
from datetime import datetime
import joblib

from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.pipeline.context import ModelArtifact, ModelSpec


class ArtifactPersistStep(PipelineStep):
    """
    Persist trained model into an immutable ModelArtifact.
    """

    stage = "training_finalize"

    def run(self, ctx: TrainingContext) -> TrainingContext:
        state = ctx.model_state
        if state is None:
            raise RuntimeError("No ModelState to persist")

        spec = ModelSpec(
            family=ctx.cfg.model_name,
            task=ctx.cfg.task_type,
            version="v1",  # ❗冻结，不再拼字符串
        )

        out_dir = Path(ctx.model_dir) / "artifacts"
        out_dir.mkdir(parents=True, exist_ok=True)

        model_path = out_dir / f"model_{state.asof_day}.joblib"
        joblib.dump(state.model, model_path)

        ctx.model_artifact = ModelArtifact(
            path=model_path,
            spec=spec,
            run_id=ctx.cfg.name,
            asof_day=state.asof_day,
            created_at=datetime.utcnow(),
            metrics=dict(ctx.metrics),
            feature_names=ctx.cfg.dataset.feature_columns,
        )

        return ctx
