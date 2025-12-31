from __future__ import annotations

from src.backtest.pipeline import BacktestPipeline
from src.utils.path import PathManager
from src.config.app_config import AppConfig
from src.observability.instrumentation import Instrumentation

from src.steps.backtest_l1_step import BacktestL1Step

from src.backtest.steps.result_step import ResultStep
from src.backtest.steps.metrics_step import MetricsStep
from src.backtest.steps.report_step import ReportStep
from src.backtest.steps.load_data_step import LoadDataStep
from src.backtest.steps.replay_l1_step import ReplayPriceL1Step


def build_offline_l1_backtest(date: str, symbol: str) -> BacktestPipeline:
    """
    Offline Level-1 Backtest (FINAL / FROZEN)

    语义：
      - 只读事实链：feature + label（通过 manifest + SliceSource）
      - 产生研究报告（fact/backtest）
      - 可随时运行，不绑定每日 data pipeline
    """
    app_cfg = AppConfig.load()  # 预留：读取 backtest config（universe, params）

    pm = PathManager()
    inst = Instrumentation()

    steps = [
        LoadDataStep(backtest_cfg=app_cfg.backtest, inst=inst),
        ReplayPriceL1Step(backtest_cfg=app_cfg.backtest, inst=inst),
        ResultStep(backtest_cfg=app_cfg.backtest, inst=inst),
        MetricsStep(inst=inst),
        ReportStep(inst=inst),
    ]

    return BacktestPipeline(
        steps=steps,
        pm=pm,
        inst=inst,
    )
