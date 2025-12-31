from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict

from src.backtest.result import BacktestResult


class MetricsCollector(ABC):
    """
    MetricsCollector (FINAL)

    BacktestResult -> metrics dict

    Backtest Result & Metrics Contract (Frozen)

BacktestResult records immutable facts only.

Metrics are pure functions of BacktestResult.

BacktestEngine is the only writer of BacktestResult.

Metrics must not affect backtest execution.

Result and Metrics must be stored separately.
    """

    @abstractmethod
    def compute(self, result: BacktestResult) -> Dict[str, float]:
        ...

import numpy as np

class BasicMetrics(MetricsCollector):
    def compute(self, result: BacktestResult) -> Dict[str, float]:
        eq = np.array(result.equity_curve)

        ret = np.diff(eq)
        sharpe = (
            np.mean(ret) / np.std(ret)
            if np.std(ret) > 0 else 0.0
        )

        drawdown = np.max(np.maximum.accumulate(eq) - eq)

        return {
            "final_equity": float(eq[-1]),
            "sharpe": float(sharpe),
            "max_drawdown": float(drawdown),
        }
class ICMetrics(MetricsCollector):
    def compute(self, result: BacktestResult) -> Dict[str, float]:
        # 假设 signals & returns 已在 result 中记录
        ...

class MetricsPipeline:
    def __init__(self, collectors: list[MetricsCollector]):
        self._collectors = collectors

    def compute(self, result: BacktestResult) -> Dict[str, float]:
        metrics = {}
        for c in self._collectors:
            metrics.update(c.compute(result))
        return metrics
