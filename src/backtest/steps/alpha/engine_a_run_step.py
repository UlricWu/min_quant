# src/backtest/steps/alpha/engine_a_run_step.py
from __future__ import annotations

from src.backtest.context import BacktestContext
from src.pipeline.step import PipelineStep
from src import logs, PathManager

from src.backtest.core.time import ReplayClock
from src.backtest.core.portfolio import Portfolio
from src.backtest.engines.alpha.engine import AlphaBacktestEngine
from src.backtest.engines.alpha.execution_sim import ExecutionSimulator
from src.backtest.engines.alpha.strategy_runner import StrategyRunner
from src.backtest.strategy.factory import StrategyFactory
from src.backtest.engines.alpha.data_view import MinuteFeatureDataView

from src.meta.symbol_slice_resolver import SymbolSliceResolver
from src.pipeline.model_artifact import resolve_model_artifact_from_dir


class EngineARunStep(PipelineStep):
    """
    EngineARunStep（FINAL / FROZEN）

    Hard rules:
    - Backtest ONLY consumes published model artifacts
    - NEVER consumes train run artifacts
    - Artifact resolution is engine responsibility
    """

    stage = "engine_a_run"

    def __init__(self, *, inst):
        super().__init__(inst=inst)
        self.pm = PathManager()

    def run(self, ctx: BacktestContext):
        logs.info(f"[EngineARunStep] date={ctx.today}")

        # --------------------------------------------------
        # 0. Resolve published model (HARD RULE)
        # --------------------------------------------------
        model_name = ctx.cfg.strategy["model"]["spec"]["artifact"]["run"]

        artifact_dir = self.pm.model_latest_dir(model_name)
        if not artifact_dir.exists():
            raise RuntimeError(
                f"[Backtest] No published model found: {artifact_dir}"
            )

        artifact = resolve_model_artifact_from_dir(artifact_dir)

        # --------------------------------------------------
        # 1. Data view (observable world)
        # --------------------------------------------------
        resolver = SymbolSliceResolver(
            meta_dir=ctx.meta_dir,
            stage="feature",
        )

        data_view = MinuteFeatureDataView(
            resolver=resolver,
            symbols=ctx.symbols,
            feature_names=artifact.feature_names,  # ✅ 唯一可信来源
            price_col="close",
        )

        # --------------------------------------------------
        # 2. Replay clock
        # --------------------------------------------------
        step_us = 60_000_000  # 60s
        start_us, end_us = data_view.time_bounds_us()
        clock = ReplayClock(
            start_us=start_us,
            end_us=end_us,
            step_us=step_us,
        )

        # --------------------------------------------------
        # 3. Portfolio
        # --------------------------------------------------
        portfolio = Portfolio(cash=1_000_000.0)

        # --------------------------------------------------
        # 4. Strategy
        # --------------------------------------------------
        from copy import deepcopy

        # --------------------------------------------------
        # 🔒 Normalize strategy config into EXECUTION form
        # --------------------------------------------------
        orig_strategy = ctx.cfg.strategy

        strategy = deepcopy(orig_strategy)
        strategy["model"] = {
            "artifact": artifact,  # ← 唯一可信来源
        }

        ctx.cfg.strategy = strategy
        # 仅消费 ModelArtifact，禁止：
        #                           - dict spec
        #                           -  run / asof / latest 语义
        #                           - 文件系统访问

        strategy = StrategyFactory.build(ctx.cfg.strategy)
        runner = StrategyRunner(strategy=strategy, symbols=ctx.symbols)

        # --------------------------------------------------
        # 5. Execution
        # --------------------------------------------------
        executor = ExecutionSimulator(data_view=data_view)

        # --------------------------------------------------
        # 6. Engine
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
