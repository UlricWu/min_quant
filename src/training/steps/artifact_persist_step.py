# src/training/steps/artifact_persist_step.py
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime
import joblib

from src import logs
from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.pipeline.model_artifact import ModelSpec, ModelArtifact


class ArtifactPersistStep(PipelineStep):
    """
    ArtifactPersistStep（FINAL / FROZEN）

    Semantics:
    - Persist run-scoped training result
    - DOES NOT publish
    - Produces ModelArtifact bound to train run
    """

    stage = "training_finalize"

    def run(self, ctx: TrainingContext) -> TrainingContext:
        state = ctx.model_state
        if state is None:
            raise RuntimeError("No ModelState to persist")

        # -----------------------------
        # Resolve artifact root (run-scoped)
        # -----------------------------
        artifact_dir = Path(ctx.model_dir)
        artifact_dir.mkdir(parents=True, exist_ok=True)

        # -----------------------------
        # Model spec (FROZEN)
        # -----------------------------
        spec = ModelSpec(
            family=ctx.cfg.model_name,
            task=ctx.cfg.task_type,
            version=ctx.cfg.model_version,  # ✅ 不再硬编码
        )

        # -----------------------------
        # Persist model
        # -----------------------------
        model_path = artifact_dir / "model.joblib"
        joblib.dump(state.model, model_path)

        # -----------------------------
        # Persist metadata
        # -----------------------------
        artifact_meta = {
            "run_id": ctx.run_id,
            "asof_day": state.asof_day,
            "created_at": datetime.utcnow().isoformat(),
            "spec": {
                "family": spec.family,
                "task": spec.task,
                "version": spec.version,
            },
            "metrics": dict(ctx.metrics),
            "feature_names": list(ctx.cfg.dataset.feature_columns),
        }

        meta_path = artifact_dir / "artifact.json"
        meta_path.write_text(
            json.dumps(artifact_meta, indent=2)
        )

        # -----------------------------
        # Attach ModelArtifact to context
        # -----------------------------
        ctx.model_artifact = ModelArtifact(
            path=artifact_dir,          # ✅ root dir
            spec=spec,
            run_id=ctx.run_id,          # ✅ 正确语义
            asof_day=state.asof_day,
            created_at=datetime.utcnow(),
            metrics=dict(ctx.metrics),
            feature_names=list(ctx.cfg.dataset.feature_columns),
        )
        logs.info(f"model_artifact={ctx.model_artifact}")

        return ctx
