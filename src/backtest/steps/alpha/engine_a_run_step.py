#!filepath: src/backtest/steps/alpha/engine_a_run_step.py
from __future__ import annotations

from src.backtest.pipeline import BacktestContext
from src.pipeline.step import PipelineStep
from src import logs

from src.backtest.core.time import ReplayClock
from src.backtest.core.portfolio import Portfolio
from src.backtest.engines.alpha.engine import AlphaBacktestEngine
from src.backtest.engines.alpha.execution_sim import ExecutionSimulator
from src.backtest.engines.alpha.strategy_runner import StrategyRunner
from src.backtest.strategy.factory import StrategyFactory
from src.backtest.engines.alpha.data_view import MinuteFeatureDataView

from src.meta.symbol_slice_resolver import SymbolSliceResolver
"""
{#!filepath: src/backtest/steps/alpha/engine_a_run_step.py}

EngineARunStep (FINAL / FROZEN)

Semantic boundary:
- This step fully encapsulates Engine A (Alpha Backtest).
- It is treated as ONE atomic experiment by the pipeline.

Responsibilities:
- Construct the observable world (MarketDataView) using meta slices.
- Initialize replay clock, portfolio, strategy, and execution model.
- Run the Alpha Backtest engine end-to-end for a given date.

Pipeline invariants:
- The pipeline MUST treat this step as a black box.
- No intermediate artifacts are exposed to the pipeline.
- World construction is an engine concern, NOT a pipeline concern.

Do NOT split DataView construction into a separate step.
That would violate engine ownership of its observable world.
"""


class EngineARunStep(PipelineStep):
    """
    Engine A 执行步（FINAL）

    - 使用 cfg.replay / cfg.strategy
    - 不知道 workflow
    """

    stage = "engine_a_run"

    def __init__(self, *, inst):
        self.inst = inst

    def run(self, ctx:BacktestContext):
        logs.info(f"[EngineARunStep] date={ctx.today}")

        resolver = SymbolSliceResolver(
            meta_dir=ctx.meta_dir,
            stage="feature",  # or cfg.level
        )

        artifact = ctx.cfg.strategy["model"]["artifact"]

        data_view = MinuteFeatureDataView(
            resolver=resolver,
            symbols=ctx.symbols,
            feature_names=artifact.feature_names,  # 🔑 唯一可信来源
            price_col="close",  # 🔑 分钟 bar 的唯一正确价格
        )

        # --------------------------------------------------
        # 1. Replay clock (MVP: fixed window)
        # --------------------------------------------------
        # MVP：先用“每分钟”步长（你后面可以再做 clock policy）
        step_us = 60_000_000  # 60s in microseconds
        start_us, end_us = data_view.time_bounds_us()
        clock = ReplayClock(
            start_us=start_us,
            end_us=end_us,
            step_us=step_us,
        )

        # --------------------------------------------------
        # 2. Portfolio (pure state)
        # --------------------------------------------------
        portfolio = Portfolio(cash=1_000_000.0)

        # --------------------------------------------------
        # 3. Strategy / Model (from cfg.strategy)
        # --------------------------------------------------
        strategy = StrategyFactory.build(ctx.cfg.strategy)
        runner = StrategyRunner(strategy=strategy, symbols=ctx.symbols)


        # --------------------------------------------------
        # 4. Execution (idealized, observable-price-based)
        # --------------------------------------------------
        executor = ExecutionSimulator(data_view=data_view)

        # --------------------------------------------------
        # 5. Engine
        # --------------------------------------------------
        engine = AlphaBacktestEngine(
            clock=clock,
            data_view=data_view,
            strategy=runner,
            portfolio=portfolio,
            executor=executor,
        )
        engine.run()

        ctx.portfolio = portfolio
        ctx.equity_curve = engine.equity_curve

        return ctx
