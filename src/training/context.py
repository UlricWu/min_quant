# src/training/context.py
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd


@dataclass
class ModelState:
    model: Any
    asof_day: str


@dataclass
class TrainingContext:
    """
    TrainingContext（FINAL / FROZEN）

    Semantics:
    - One context == one training run
    - run_id is immutable and mandatory
    """

    # -------------------------
    # Identity (FROZEN)
    # -------------------------
    run_id: str

    # -------------------------
    # Static bindings
    # -------------------------
    cfg: Any
    inst: Any
    model_dir: Path

    # -------------------------
    # Rolling state
    # -------------------------
    update_day: str = ""
    eval_day: str = ""

    train_X: Optional[pd.DataFrame] = None
    train_y: Optional[pd.Series] = None

    eval_X: Optional[pd.DataFrame] = None
    eval_y: Optional[pd.Series] = None

    model_state: Optional[ModelState] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
