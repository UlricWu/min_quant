# src/backtest/report/equity_curve.py
import matplotlib.pyplot as plt

from src.backtest.report.base import Report
from src.backtest.result import BacktestResult


class EquityCurveReport(Report):
    def __init__(self, output_path):
        self._path = output_path

    def render(self, result: BacktestResult) -> None:
        plt.figure(figsize=(10, 4))
        plt.plot(result.timestamps, result.equity_curve)
        plt.title(f"Equity Curve: {result.name}")
        plt.xlabel("Time")
        plt.ylabel("Equity")
        plt.tight_layout()
        plt.savefig(self._path)
        plt.close()

import json

class MetricsReport(Report):
    def __init__(self, metrics: dict, output_path):
        self._metrics = metrics
        self._path = output_path

    def render(self, result: BacktestResult) -> None:
        with open(self._path, "w") as f:
            json.dump(self._metrics, f, indent=2)
import pandas as pd

class TradesReport(Report):
    def __init__(self, output_path):
        self._path = output_path

    def render(self, result: BacktestResult) -> None:
        if not result.trades:
            return

        df = pd.DataFrame(result.trades)
        df.to_csv(self._path, index=False)
class ReportPipeline:
    def __init__(self, reports: list[Report]):
        self._reports = reports

    def render_all(self, result: BacktestResult):
        for r in self._reports:
            r.render(result)

