# tests/conftest.py
from __future__ import annotations

import multiprocessing
from pathlib import Path

import pytest
from loguru import logger


# -----------------------------------------------------------------------------
# Global test hygiene
# -----------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def disable_file_logger():
    logger.remove()
    logger.add(lambda msg: None)
    yield


@pytest.fixture(scope="session", autouse=True)
def _set_start_method():
    multiprocessing.set_start_method("spawn", force=True)


# -----------------------------------------------------------------------------
# Generic filesystem helpers
# -----------------------------------------------------------------------------

@pytest.fixture
def tmp_dirs(tmp_path: Path) -> dict[str, Path]:
    """
    Unified directory layout factory.

    This fixture does NOT assume any pipeline.
    It only prepares a clean filesystem namespace.
    """
    dirs = {
        "raw": tmp_path / "raw",
        "fact": tmp_path / "fact",
        "feature": tmp_path / "feature",
        "meta": tmp_path / "meta",
        "label": tmp_path / "label",
        "model": tmp_path / "model",
        "report": tmp_path / "report",
    }

    for d in dirs.values():
        d.mkdir(parents=True, exist_ok=True)

    return dirs
