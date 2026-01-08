#!filepath: src/backtest/pipeline.py
from __future__ import annotations

from typing import List

from src import logs, PathManager
from src.backtest.context import BacktestContext
from src.config.backtest_config import BacktestConfig
from src.observability.instrumentation import Instrumentation
from src.pipeline.step import PipelineStep


class BacktestPipeline:
    """
    BacktestPipeline（FINAL / FROZEN）

    Semantics:
    - Owns date iteration
    - Orchestrates steps in semantic order
    - Does NOT interpret strategy/model internals
    - Does NOT touch training artifacts
    """

    def __init__(
        self,
        *,
        daily_steps: List[PipelineStep],
        final_steps: List[PipelineStep],
        pm: PathManager,
        inst: Instrumentation,
        cfg: BacktestConfig,
    ):
        self.daily_steps = daily_steps
        self.final_steps = final_steps
        self.pm = pm
        self.inst = inst
        self.cfg = cfg

    def run(self, run_id: str) -> BacktestContext:
        logs.info(f"[BacktestPipeline] START run_id={run_id}")

        ctx = BacktestContext(
            cfg=self.cfg,
            inst=self.inst,
            symbols=list(self.cfg.symbols),
            run_id=run_id,
        )

        # -----------------------------------
        # Daily loop (pipeline-owned)
        # -----------------------------------
        for today in self.cfg.dates:
            ctx.today = today
            ctx.meta_dir = self.pm.meta_dir(today)

            # reset per-day runtime/resolved state
            ctx.data_view = None
            ctx.runtime = None
            ctx.portfolio = None
            ctx.equity_curve = None

            for step in self.daily_steps:
                ctx = step.run(ctx)

        # -----------------------------------
        # Final steps (after all days)
        # -----------------------------------
        for step in self.final_steps:
            ctx = step.run(ctx)

        logs.info("[BacktestPipeline] DONE")
        return ctx
