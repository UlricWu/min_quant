# src/config/training_config.py
from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class FeatureLabelConfig(BaseModel):
    feature_columns: List[str]
    label_column: str
    drop_na: bool = True


class TrainingConfig(BaseModel):
    """
    TrainingConfig（ONLINE / FINAL / FROZEN）
    """

    # experiment
    name: str

    # date range
    start_date: date
    end_date: date
    warmup_days: int = 20
    step_unit: Literal["day"] = "day"

    # dataset
    dataset: FeatureLabelConfig

    # model
    task_type: Literal["regression"] = "regression"
    model_name: str = "sgd"
    model_version: str = "regressor_v1"
    model_params: Dict[str, Any] = Field(default_factory=dict)

    # evaluation
    evaluation_enabled: bool = True
    evaluation_metrics: List[str] = Field(
        default_factory=lambda: ["ic"]
    )

    # snapshot
    snapshot_enabled: bool = False
    snapshot_every_n_steps: int = 1
