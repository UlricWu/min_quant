#!filepath: src/workflows/offline_l1_backtest.py
from __future__ import annotations

from src.config.app_config import AppConfig
from src.observability.instrumentation import Instrumentation
from src.utils.path import PathManager

from src.backtest.pipeline import BacktestPipeline

"""
Offline Level-1 Backtest Workflow（FINAL / FROZEN）

Design invariant:
- Workflow ONLY wires steps in semantic order.
- Workflow does NOT interpret config.
- Backtest consumes ONLY published models (namespace contract).
"""

from src.backtest.steps.alpha.resolve_published_model_step import ResolvePublishedModelStep
from src.backtest.steps.alpha.build_minute_dataview_step import BuildMinuteDataViewStep
from src.backtest.steps.alpha.build_execution_runtime_step import BuildExecutionRuntimeStep
from src.backtest.steps.alpha.run_alpha_engine_step import RunAlphaEngineStep
from src.backtest.steps.alpha.engine_a_report_step import EngineAReportStep


def build_offline_l1_backtest(
    *,
    cfg=None,
) -> BacktestPipeline:
    if cfg is None:
        cfg = AppConfig.load().backtest
    pm = PathManager()
    inst = Instrumentation()

    return BacktestPipeline(
        daily_steps=[
            ResolvePublishedModelStep(inst=inst, pm=pm),
            BuildMinuteDataViewStep(inst=inst),
            BuildExecutionRuntimeStep(inst=inst),
            RunAlphaEngineStep(inst=inst),
        ],
        final_steps=[
            EngineAReportStep(inst=inst),
        ],
        pm=pm,
        inst=inst,
        cfg=cfg,
    )
