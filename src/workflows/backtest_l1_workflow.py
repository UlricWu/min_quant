#!filepath: src/workflows/backtest_l1_workflow.py
from __future__ import annotations

from dataclasses import dataclass

from src.config.app_config import AppConfig
from src.utils.path import PathManager
from src.utils.logger import logs

from src.backtest.data_handler import BarDataHandler
from src.backtest.strategy import SimpleThresholdStrategy
from src.backtest.execution import ExecutionEngine
from src.backtest.portfolio import Portfolio
from src.backtest.backtest import BacktestEngine


@dataclass(frozen=True)
class BacktestL1Spec:
    date: str
    cash: float = 1_000_000.0
    qty: int = 100
    feature_name: str = "l1_norm_ret"  # 你需要换成项目里真实存在的某列
    threshold: float = 0.0


def run_backtest_l1(spec: BacktestL1Spec) -> None:
    cfg = AppConfig.load()
    pm = PathManager()

    # 选取需要的特征列（只取 strategy 用到的最小集合）

    data = BarDataHandler(
        pm=pm,
        date=spec.date,
    )

    strategy = SimpleThresholdStrategy(
        feature_name=spec.feature_name,
        threshold=spec.threshold,
    )

    execution = ExecutionEngine()
    portfolio = Portfolio(cash=spec.cash)

    bt = BacktestEngine(
        data_handler=data,
        strategy=strategy,
        execution=execution,
        portfolio=portfolio,
    )
    bt.run()


if __name__ == "__main__":
    # Example:
    # python -m src.workflows.backtest_l1_workflow
    run_backtest_l1(
        BacktestL1Spec(
            date="2025-12-01",
            feature_name="l1_norm_ret",  # 改成你真实的 l1 列名
            threshold=0.0,
        )
    )
