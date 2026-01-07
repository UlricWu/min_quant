#!filepath: tests/backtest/strategy/test_strategy_factory_frozen.py
from __future__ import annotations

import copy
import inspect
from pathlib import Path

import joblib
import pytest

from src.backtest.strategy.factory import StrategyFactory
from src.backtest.strategy.base import Strategy
from src.pipeline.model_artifact import ModelArtifact, ModelSpec


# =============================================================================
# Dummy execution model (MUST be module-level)
# =============================================================================

class DummyModel:
    """
    Pickle-safe dummy inference model.

    FROZEN:
    - Defined at module level
    - Has predict()
    """

    def predict(self, X):
        return [0.0] * len(X)


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def dummy_artifact(tmp_path: Path) -> ModelArtifact:
    """
    Minimal *real* ModelArtifact for execution-domain tests.

    HARD REQUIREMENTS:
    - model.joblib must be loadable by joblib
    - model must have predict()
    """

    artifact_dir = tmp_path / "artifact"
    artifact_dir.mkdir(parents=True)

    model_path = artifact_dir / "model.joblib"

    # 🔒 REAL joblib artifact (pickle-safe)
    joblib.dump(DummyModel(), model_path)

    return ModelArtifact(
        path=artifact_dir,
        spec=ModelSpec(
            family="sgd",
            task="regression",
            version="v1",
        ),
        run_id="exp_test",
        asof_day="2025-11-04",
        metrics=None,
        created_at=None,
        feature_names=["open", "close"],
    )


# =============================================================================
# Contract tests
# =============================================================================

def test_missing_type_raises():
    cfg = {"params": {"threshold": 0.1}}

    with pytest.raises(KeyError, match="missing 'type'"):
        StrategyFactory.build(cfg)


def test_unknown_type_raises():
    cfg = {"type": "unknown_strategy", "params": {}}

    with pytest.raises(ValueError, match="unknown strategy type"):
        StrategyFactory.build(cfg)


def test_missing_model_artifact_raises():
    """
    🔒 FROZEN:
    StrategyFactory MUST reject declarative dicts.
    """
    cfg = {
        "type": "threshold",
        "model": {},  # ❌ no artifact
        "params": {"threshold": 0.0},
    }

    with pytest.raises(KeyError, match="model.artifact"):
        StrategyFactory.build(cfg)


def test_model_artifact_must_be_object():
    """
    🔒 FROZEN:
    model.artifact must be ModelArtifact, not dict.
    """
    cfg = {
        "type": "threshold",
        "model": {
            "artifact": {"run": "xxx", "asof": "latest"}
        },
        "params": {"threshold": 0.0},
    }

    with pytest.raises(TypeError, match="ModelArtifact"):
        StrategyFactory.build(cfg)


def test_build_returns_strategy(dummy_artifact):
    """
    Execution World:
    - StrategyFactory returns a Strategy
    - Model is injected internally
    """
    cfg = {
        "type": "threshold",
        "model": {"artifact": dummy_artifact},
        "params": {"threshold": 0.0, "qty": 1},
    }

    strategy = StrategyFactory.build(cfg)

    assert isinstance(strategy, Strategy)


def test_registry_not_empty():
    registry = StrategyFactory._REGISTRY

    assert isinstance(registry, dict)
    assert len(registry) > 0


def test_registry_not_mutated_during_build(dummy_artifact):
    original = StrategyFactory._REGISTRY.copy()

    cfg = {
        "type": "threshold",
        "model": {"artifact": dummy_artifact},
        "params": {"threshold": 0.0},
    }

    StrategyFactory.build(cfg)

    assert StrategyFactory._REGISTRY == original


def test_cfg_not_modified(dummy_artifact):
    cfg = {
        "type": "threshold",
        "model": {"artifact": dummy_artifact},
        "params": {"threshold": 0.1},
    }

    cfg_copy = copy.deepcopy(cfg)

    StrategyFactory.build(cfg)

    assert cfg == cfg_copy


def test_no_branching_on_strategy_type():
    """
    🔒 FROZEN:
    Strategy selection must be registry-based, not if/else.
    """
    src = inspect.getsource(StrategyFactory.build)

    forbidden = ["if typ ==", "elif", "switch", "case"]

    for kw in forbidden:
        assert kw not in src, f"Branching logic '{kw}' found in StrategyFactory.build"
