# src/training/context.py
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


@dataclass
class ModelState:
    model: Any
    asof_day: pd.Timestamp


@dataclass
class TrainingContext:
    cfg: Any
    inst: Any

    update_day: pd.Timestamp
    eval_day: Optional[pd.Timestamp]

    model_dir: Path

    train_X: Optional[pd.DataFrame] = None
    train_y: Optional[pd.Series] = None

    eval_X: Optional[pd.DataFrame] = None
    eval_y: Optional[pd.Series] = None

    model_state: Optional[ModelState] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
