#!filepath: src/backtest/steps/alpha/run_alpha_engine_step.py
from __future__ import annotations

from src import logs
from src.backtest.context import BacktestContext
from src.backtest.engines.alpha.engine import AlphaBacktestEngine
from src.pipeline.step import PipelineStep


class RunAlphaEngineStep(PipelineStep):
    """
    RunAlphaEngineStep（FINAL / FROZEN）

    Contract:
    - Engine is pure execution:
        consumes runtime objects, produces results
    - Engine must NOT access filesystem or config
    - Output: ctx.portfolio, ctx.equity_curve
    """

    stage = "run_engine"

    def run(self, ctx: BacktestContext) -> BacktestContext:
        if ctx.data_view is None:
            raise RuntimeError("[RunAlphaEngineStep] data_view is not built")
        if ctx.runtime is None:
            raise RuntimeError("[RunAlphaEngineStep] runtime is not built")

        logs.info(f"[RunAlphaEngineStep] date={ctx.today}")

        engine = AlphaBacktestEngine(
            clock=ctx.runtime.clock,
            data_view=ctx.data_view,
            strategy=ctx.runtime.strategy_runner,
            portfolio=ctx.runtime.portfolio,
            executor=ctx.runtime.executor,
        )
        engine.run()

        ctx.portfolio = ctx.runtime.portfolio
        ctx.equity_curve = engine.equity_curve
        return ctx
