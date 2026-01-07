# tests/backtest/conftest.py
from __future__ import annotations

from pathlib import Path
import pytest

from src.backtest.context import BacktestContext
from src.observability.instrumentation import Instrumentation
from src.config.backtest_config import BacktestConfig


@pytest.fixture
def backtest_cfg():
    return BacktestConfig(
        name="test_backtest",
        level="l1",
        replay="single",
        dates=["2025-01-01"],
        symbols=["AAA", "BBB"],
        strategy={},
    )


@pytest.fixture
def backtest_ctx(tmp_dirs, backtest_cfg) -> BacktestContext:
    """
    Minimal BacktestContext for execution tests.
    """
    return BacktestContext(
        cfg=backtest_cfg,
        inst=Instrumentation(enabled=False),
        symbols=backtest_cfg.symbols,
        today="2025-01-01",
        meta_dir=tmp_dirs["meta"],
        portfolio=None,
        equity_curve=None,
        report=None,
    )
