#!filepath: src/backtest/steps/alpha/engine_a_run_step.py
from __future__ import annotations

from src.pipeline.step import PipelineStep
from src import logs

from src.backtest.core.time import ReplayClock
from src.backtest.core.portfolio import Portfolio
from src.backtest.engines.alpha.engine import AlphaBacktestEngine
from src.backtest.engines.alpha.execution_sim import ExecutionSimulator
from src.backtest.engines.alpha.strategy_runner import StrategyRunner
from src.backtest.strategy.registry import StrategyFactory


class EngineARunStep(PipelineStep):
    """
    Engine A 执行步（FINAL）

    - 使用 cfg.replay / cfg.strategy
    - 不知道 workflow
    """

    stage = "engine_a_run"

    def __init__(self, *, inst):
        self.inst = inst

    def run(self, ctx):
        logs.info(f"[EngineARunStep] date={ctx.date}")

        # --------------------------------------------------
        # 1. Replay clock (MVP: fixed window)
        # --------------------------------------------------
        clock = ReplayClock(
            start_us=0,
            end_us=5_000_000,
            step_us=1_000_000,
        )

        # --------------------------------------------------
        # 2. Portfolio (pure state)
        # --------------------------------------------------
        portfolio = Portfolio(cash=1_000_000.0)

        # --------------------------------------------------
        # 3. Strategy / Model (from cfg.strategy)
        # --------------------------------------------------
        model, strategy = StrategyFactory.build(ctx.cfg.strategy)
        runner = StrategyRunner(model=model, strategy=strategy, symbols=ctx.symbols)


        # --------------------------------------------------
        # 4. Execution (idealized, observable-price-based)
        # --------------------------------------------------
        executor = ExecutionSimulator(data_view=ctx.data_view)

        # --------------------------------------------------
        # 5. Engine
        # --------------------------------------------------
        engine = AlphaBacktestEngine(
            clock=clock,
            data_view=ctx.data_view,
            strategy=runner,
            portfolio=portfolio,
            executor=executor,
        )
        engine.run()

        ctx.portfolio = portfolio
        ctx.equity_curve = engine.equity_curve
        return ctx
