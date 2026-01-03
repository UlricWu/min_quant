# tests/backtest/strategy/test_strategy_factory_frozen.py
from __future__ import annotations

import pytest

from src.backtest.strategy.factory import StrategyFactory
from src.backtest.strategy.base import Strategy, Model
def test_strategy_factory_missing_type_raises():
    cfg = {
        "params": {"threshold": 0.1}
    }

    with pytest.raises(KeyError, match="missing 'type'"):
        StrategyFactory.build(cfg)
def test_strategy_factory_unknown_type_raises():
    cfg = {
        "type": "unknown_strategy",
        "params": {}
    }

    with pytest.raises(ValueError, match="unknown strategy type"):
        StrategyFactory.build(cfg)
def test_strategy_factory_build_returns_model_and_strategy():
    cfg = {
        "type": "threshold",
        "model": {},
        "params": {
            "threshold": 0.0,
            "qty": 1,
        },
    }

    model, strategy = StrategyFactory.build(cfg)

    assert isinstance(model, Model)
    assert isinstance(strategy, Strategy)
def test_strategy_factory_registry_not_empty():
    registry = StrategyFactory._REGISTRY

    assert isinstance(registry, dict)
    assert len(registry) > 0
def test_strategy_factory_registry_is_not_mutated_during_build():
    original_registry = StrategyFactory._REGISTRY.copy()

    cfg = {
        "type": "threshold",
        "model": {},
        "params": {},
    }

    StrategyFactory.build(cfg)

    assert StrategyFactory._REGISTRY == original_registry
def test_strategy_factory_does_not_modify_cfg():
    cfg = {
        "type": "threshold",
        "model": {},
        "params": {"threshold": 0.1},
    }

    cfg_copy = cfg.copy()

    StrategyFactory.build(cfg)

    assert cfg == cfg_copy
import inspect


def test_strategy_factory_has_no_branching_logic():
    src = inspect.getsource(StrategyFactory.build)

    forbidden = ["if typ ==", "elif", "switch", "case"]

    for kw in forbidden:
        assert kw not in src, f"Branching logic '{kw}' found in StrategyFactory.build"
