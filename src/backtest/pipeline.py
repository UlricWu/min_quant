# src/backtest/pipeline.py
from __future__ import annotations

from typing import List

from src import logs, PathManager
from src.config.backtest_config import BacktestConfig
from src.observability.instrumentation import Instrumentation
from src.pipeline.step import PipelineStep
from src.backtest.context import BacktestContext


class BacktestPipeline:
    """
    BacktestPipeline（FINAL / FROZEN）

    Semantics:
    - Pipeline owns date iteration
    - Pipeline NEVER touches training artifacts
    - Pipeline NEVER resolves model artifacts
    - Pipeline only orchestrates steps
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

        # NOTE:
        # - run_id here is ONLY a backtest execution id
        # - It has NO relation to training runs
        ctx = BacktestContext(
            cfg=self.cfg,
            inst=self.inst,
            symbols=list(self.cfg.symbols),
        )

        for today in self.cfg.dates:
            ctx.today = today
            ctx.meta_dir = self.pm.meta_dir(today)

            for step in self.daily_steps:
                ctx = step.run(ctx)

        for step in self.final_steps:
            ctx = step.run(ctx)

        logs.info("[BacktestPipeline] DONE")
        return ctx
