from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

from src.pipeline.context import BaseContext


@dataclass
class DataContext(BaseContext):
    """
    DataContext (FINAL / FROZEN)
    """

    today: str

    raw_dir: Path
    fact_dir: Path
    meta_dir: Path
    feature_dir: Path
    label_dir: Path
