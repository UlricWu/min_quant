# src/core/model_artifact.py
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from typing import Literal, Any

from src import logs
from src.utils.path import PathManager


# ============================================================
# Model Spec (FROZEN)
# ============================================================
@dataclass(frozen=True)
class ModelSpec:
    family: Literal["sgd"]
    task: Literal["regression", "classification"]
    version: str


# ============================================================
# Model Artifact (RUN-SCOPED OR PUBLISHED)
# ============================================================
@dataclass(frozen=True)
class ModelArtifact:
    """
    ModelArtifact（FINAL / FROZEN）

    Semantics:
    - path always points to an artifact ROOT directory
    - NEVER points to a single file
    """
    path: Path
    spec: ModelSpec
    run_id: str | None = None          # training run id (only for run-scoped)
    asof_day: str | None = None
    metrics: dict[str, Any] | None = None
    created_at: datetime | None = None
    feature_names: list[str] | None = None


# ============================================================
# Resolve PUBLISHED model artifact (ONLY)
# ============================================================
def resolve_model_artifact_from_dir(artifact_dir: Path) -> ModelArtifact:
    """
    Resolve a published ModelArtifact from directory.

    FINAL / FROZEN

    Hard rules:
    - artifact_dir MUST be a published model directory
    - NEVER accepts train run directories
    """
    meta_path = artifact_dir / "artifact.json"
    if not meta_path.exists():
        raise RuntimeError(
            f"[ModelArtifact] artifact.json not found in {artifact_dir}"
        )

    meta = json.loads(meta_path.read_text())

    spec = ModelSpec(
        family=meta["spec"]["family"],
        task=meta["spec"]["task"],
        version=meta["spec"]["version"],
    )

    return ModelArtifact(
        path=artifact_dir,
        spec=spec,
        run_id=meta.get("run_id"),
        asof_day=meta.get("asof_day"),
        metrics=meta.get("metrics"),
        created_at=datetime.fromisoformat(meta["created_at"]),
        feature_names=meta.get("feature_names"),
    )


# ============================================================
# Promote RUN-SCOPED artifact → PUBLISHED model
# ============================================================
def promote_model_artifact(
    *,
    artifact: ModelArtifact,
    model_name: str,
    version: str | None = None,
):
    """
    Promote a run-scoped ModelArtifact into model lineage space.

    FINAL / FROZEN

    Semantics:
    - ONLY called from ExperimentPipeline
    - Writes into shared/models/*
    - Updates `latest` symlink
    """
    pm = PathManager()

    run_dir = artifact.path
    if not run_dir.exists():
        raise RuntimeError(
            f"[ArtifactPromote] run artifact dir not found: {run_dir}"
        )

    # Default version = today (YYYY-MM-DD)
    if version is None:
        version = datetime.now().strftime("%Y-%m-%d")

    lineage_dir = pm.model_lineage_dir(model_name)
    target = pm.model_version_dir(model_name, version)
    target.mkdir(parents=True, exist_ok=True)

    logs.info(
        f"[ArtifactPromote] promote {run_dir.name} → "
        f"{model_name}/{version}"
    )

    shutil.copytree(run_dir, target, dirs_exist_ok=True)

    latest = pm.model_latest_dir(model_name)
    if latest.exists() or latest.is_symlink():
        latest.unlink()
    latest.symlink_to(target.name)
