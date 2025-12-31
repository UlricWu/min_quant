# src/backtest/report/base.py
from __future__ import annotations
from abc import ABC, abstractmethod

from src.backtest.result import BacktestResult


class Report(ABC):
    """
    Report (FINAL / FROZEN)

    BacktestResult -> side effects (files, figures)

    Report / Visualization Contract (Frozen)

Reports are read-only consumers of BacktestResult.

Reports must not alter backtest execution or metrics.

All derived analytics must be computed in Metrics layer.

Reports produce only external artifacts (files, figures).

Deleting reports must not affect reproducibility.
    """

    @abstractmethod
    def render(self, result: BacktestResult) -> None:
        ...
