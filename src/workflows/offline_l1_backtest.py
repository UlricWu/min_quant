
# !filepath: src/workflows/offline_l1_backtest.py
from src.backtest.pipeline import BacktestPipeline
from src.utils.path import PathManager
from src.observability.instrumentation import Instrumentation
from src.config.app_config import AppConfig

from src.backtest.steps.write_run_manifest_step import WriteRunManifestStep
from src.backtest.steps.load_data_step import LoadDataStep
from src.backtest.steps.replay_l1_step import ReplayPriceL1Step
from src.backtest.steps.finalize_result_step import FinalizeResultStep
from src.backtest.steps.metrics_step import MetricsStep
from src.backtest.steps.report_step import ReportStep

def build_offline_l1_backtest() -> BacktestPipeline:
    """
    Offline Level-1 Backtest (FINAL / FROZEN)

    语义：
      - 只读事实链：feature + label（通过 manifest + SliceSource）
      - 产生研究报告（fact/backtest）
      - 可随时运行，不绑定每日 data_handler pipeline
    """
    cfg = AppConfig.load().backtest
    pm = PathManager()
    inst = Instrumentation()

    return BacktestPipeline(
        daily_steps=[
            LoadDataStep(inst=inst),
            ReplayPriceL1Step(inst=inst),
        ],
        final_steps=[
            FinalizeResultStep(inst=inst),
            MetricsStep(inst=inst),
            ReportStep(inst=inst),
        ],
        pm=pm,
        inst=inst,
        cfg=cfg,
    )

