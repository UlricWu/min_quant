# src/backtest/context.py
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Any

from src.pipeline.context import BaseContext
from src.config.backtest_config import BacktestConfig
from src.observability.instrumentation import Instrumentation

# 运行期对象（只在 backtest 期间存在）
from src.backtest.core.portfolio import Portfolio
from src.backtest.engines.alpha.data_view import MinuteFeatureDataView
from src.pipeline.model_artifact import ModelArtifact


@dataclass(slots=True)
class ExecutionRuntime:
    """
    ExecutionRuntime（FROZEN）

    Contract:
    - Pure runtime state for a single (date, model, strategy) execution.
    - Must NOT store config / filesystem paths / publish semantics.
    """
    clock: Any
    portfolio: Portfolio
    executor: Any
    strategy_runner: Any


@dataclass
class BacktestContext(BaseContext):
    """
    BacktestContext（FROZEN）

    Contract:
    - cfg is READ-ONLY.
    - ctx fields are explicitly declared (slots=True).
    - No step is allowed to mutate cfg.
    """

    # injected once
    cfg: BacktestConfig
    inst: Instrumentation
    symbols: list[str]

    # run-scoped
    run_id: str

    # resolved by steps
    model_artifact: ModelArtifact | None = None
    data_view: MinuteFeatureDataView | None = None
    runtime: ExecutionRuntime | None = None

    # results
    portfolio: Portfolio | None = None
    equity_curve: Any | None = None
    metrics: dict[str, Any] | None = None
