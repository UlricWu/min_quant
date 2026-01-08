#!filepath: src/config/backtest_config.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class BacktestModelSpec:
    """
    BacktestModelSpec（FROZEN）

    selector:
      - published_latest: consume current published model snapshot
      - published_asof:   consume published snapshot as of ctx.today
    """
    name: str
    selector: str = "published_latest"


@dataclass(frozen=True, slots=True)
class BacktestConfig:
    """
    BacktestConfig（FROZEN）

    Notes:
    - strategy stays as dict for now (engine-internal interpretation).
    - model spec is explicit to avoid implicit 'latest' assumptions.
    """
    name: str
    dates: list[str]
    symbols: list[str]
    strategy: dict[str, Any]

    # NEW (recommended)
    model: BacktestModelSpec | None = None
