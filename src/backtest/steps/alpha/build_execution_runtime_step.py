#!filepath: src/backtest/steps/alpha/build_execution_runtime_step.py
from __future__ import annotations

from copy import deepcopy

from src import logs
from src.backtest.context import BacktestContext, ExecutionRuntime
from src.backtest.core.time import ReplayClock
from src.backtest.core.portfolio import Portfolio
from src.backtest.engines.alpha.execution_sim import ExecutionSimulator
from src.backtest.engines.alpha.strategy_runner import StrategyRunner
from src.backtest.strategy.factory import StrategyFactory
from src.pipeline.step import PipelineStep


class BuildExecutionRuntimeStep(PipelineStep):
    """
    BuildExecutionRuntimeStep（FINAL / FROZEN）

    Contract:
    - Construct all *runtime* objects for one day:
        clock / portfolio / strategy_runner / executor
    - Must NOT mutate ctx.cfg
    - Output: ctx.runtime
    """

    stage = "build_runtime"

    def run(self, ctx: BacktestContext) -> BacktestContext:
        if ctx.data_view is None:
            raise RuntimeError("[BuildExecutionRuntimeStep] data_view is not built")
        if ctx.model_artifact is None:
            raise RuntimeError("[BuildExecutionRuntimeStep] model_artifact is not resolved")

        logs.info(f"[BuildExecutionRuntimeStep] date={ctx.today}")

        # 1) clock
        step_us = 60_000_000  # 60s
        start_us, end_us = ctx.data_view.time_bounds_us()
        clock = ReplayClock(
            start_us=start_us,
            end_us=end_us,
            step_us=step_us,
        )

        # 2) portfolio
        portfolio = Portfolio(cash=1_000_000.0)

        # 3) strategy (execution form) - cfg remains read-only
        #    The ONLY trusted model source is ctx.model_artifact.
        strategy_cfg = deepcopy(ctx.cfg.strategy)
        strategy_cfg["model"] = {"artifact": ctx.model_artifact}

        strategy = StrategyFactory.build(strategy_cfg)
        runner = StrategyRunner(strategy=strategy, symbols=ctx.symbols)

        # 4) executor
        executor = ExecutionSimulator(data_view=ctx.data_view)

        ctx.runtime = ExecutionRuntime(
            clock=clock,
            portfolio=portfolio,
            executor=executor,
            strategy_runner=runner,
        )
        return ctx
