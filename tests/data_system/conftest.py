# tests/data/conftest.py
from __future__ import annotations

from pathlib import Path
import pytest

from src.data_system.context import DataContext


@pytest.fixture
def data_ctx(tmp_dirs) -> DataContext:
    """
    Minimal DataContext for data pipeline tests.

    Scope:
    - feature build
    - normalize
    - fact generation
    """
    return DataContext(
        today="2025-01-01",
        raw_dir=tmp_dirs["raw"],
        fact_dir=tmp_dirs["fact"],
        feature_dir=tmp_dirs["feature"],
        meta_dir=tmp_dirs["meta"],
        label_dir=tmp_dirs["label"],
        normalized_dir=tmp_dirs.get("normalized", tmp_dirs["raw"]),
    )
