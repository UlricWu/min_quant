# tests/training/conftest.py
from __future__ import annotations

from pathlib import Path
import pytest

from src.training.context import TrainingContext
from src.observability.instrumentation import Instrumentation


@pytest.fixture
def training_ctx(tmp_dirs) -> TrainingContext:
    """
    Minimal TrainingContext for training pipeline tests.
    """
    return TrainingContext(
        cfg=None,  # tests inject if needed
        inst=Instrumentation(enabled=False),
        model_dir=tmp_dirs["model"],
    )
