from __future__ import annotations

from typing import Literal

# src/core/model_spec.py
# src/core/model_artifact.py
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from datetime import datetime


@dataclass(frozen=True)
class ModelSpec:
    family: Literal["sgd"]
    task: Literal["regression", "classification"]
    version: str


@dataclass(frozen=True)
class ModelArtifact:
    path: Path
    spec: ModelSpec
    run_id: str | None = None
    asof_day: str | None = None
    metrics: dict[str, Any] | None = None
    created_at: datetime | None = None
    feature_names: list[str] | None = None# ⭐ 新增（非常关键）


@dataclass
class BaseContext:
    """
    BaseContext (FINAL / FROZEN)

    Infrastructure marker base class.

    Design invariants:
    - Defines NO dataclass fields
    - Carries no business or runtime data
    - Exists only for type identity
    """
    pass
