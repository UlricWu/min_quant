from __future__ import annotations

from src.config.app_config import AppConfig
from src.observability.instrumentation import Instrumentation
from src.utils.path import PathManager

from src.backtest.pipeline import BacktestPipeline

"""
{#!filepath: src/workflows/offline_l1_backtest.py}

Offline Level-1 Backtest Workflow (FINAL / FROZEN)

Design invariant:
- Workflow ONLY orchestrates experiment execution.
- BacktestConfig defines WHAT experiment to run.
- Workflow does NOT interpret config fields.
- Replay, data access, and strategy are engine-internal concerns.

Pipeline semantics:
- Stage 1: Run Alpha Backtest (EngineARunStep)
- Stage 2: Generate research reports

The workflow must remain stable even as engines evolve.
"""

from src.backtest.steps.alpha.engine_a_run_step import EngineARunStep
from src.backtest.steps.alpha.engine_a_report_step import EngineAReportStep


def build_offline_l1_backtest() -> BacktestPipeline:
    """
    Build Offline Level-1 Backtest Pipeline (Engine A).

    Contract (FROZEN):
    - Accepts BacktestConfig via AppConfig.load().backtest
    - Does NOT loop over dates
    - Does NOT inspect symbols / strategy / replay policy
    - Merely wires steps in correct semantic order
    """

    # --------------------------------------------------
    # Global injection (single source of truth)
    # --------------------------------------------------
    cfg = AppConfig.load().backtest  # <- BacktestConfig (experiment definition)
    pm = PathManager()
    inst = Instrumentation()

    # --------------------------------------------------
    # Engine A wiring (semantic order is FROZEN)
    # --------------------------------------------------
    return BacktestPipeline(
        daily_steps=[
            # 1) Resolve MarketDataView for (date, symbols)
            #    - uses cfg.symbols
            #    - uses manifest + meta.slice_accessor
            # EngineADataViewStep(inst=inst),

            # 2) Run Alpha Backtest
            #    - replay policy (cfg.replay)
            #    - minute boundary decision
            #    - strategy/model resolved from cfg.strategy
            EngineARunStep(inst=inst),
        ],
        final_steps=[
            # 3) Research artifacts
            EngineAReportStep(inst=inst),
        ],
        pm=pm,
        inst=inst,
        cfg=cfg,  # <- BacktestConfig injected once, read-only everywhere
    )
