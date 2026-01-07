# src/backtest/context.py
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from src.pipeline.context import BaseContext
from src.config.backtest_config import BacktestConfig
from src.observability.instrumentation import Instrumentation


@dataclass
class BacktestContext(BaseContext):
    """
    BacktestContext (FINAL / FROZEN)

    Owns runtime state for backtest system.
    """

    cfg: BacktestConfig
    inst: Instrumentation

    # per-run
    symbols: List[str]

    # per-date
    today: Optional[str] = None
    meta_dir: Optional[Path] = None

    # engine outputs
    portfolio: Optional[object] = None
    equity_curve: Optional[object] = None
    report: Optional[object] = None
