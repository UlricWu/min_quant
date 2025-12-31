# src/backtest/steps/backtest_l1_step.py
from pathlib import Path

from src.pipeline.context import BacktestContext
from src.pipeline.step import PipelineStep
from src import logs

from src.meta.symbol_slice_resolver import SymbolSliceResolver

from src.backtest.engine import BacktestEngine
from src.backtest.replay.single_symbol import SingleSymbolReplay
from src.backtest.replay.multi_symbol import MultiSymbolReplay
from src.backtest.data.feature_data_handler import FeatureDataHandler
from src.backtest.strategy.threshold import ThresholdStrategy
from src.backtest.portfolio.l1 import L1Portfolio


class BacktestL1Step(PipelineStep):
    """
    BacktestL1Step (FINAL / FROZEN)

    L1 Signal-Driven Backtest
    - single / multi symbol unified
    """

    stage = "backtest_l1"
    output_slot = "research"

    def __init__(self, *, backtest_cfg, inst=None):
        super().__init__(inst=inst)
        self._bt = backtest_cfg

    def run(self, ctx: BacktestContext):
        symbols = self._bt.symbols
        strategy_cfg = self._bt.strategy
        replay_mode = self._bt.replay

        # ------------------------------
        # ① 从 config 取 feature 定义
        # ------------------------------
        try:
            feature_cols = strategy_cfg["features"]
            signal_feature = strategy_cfg["signal_feature"]
            threshold = strategy_cfg["threshold"]
        except KeyError as e:
            raise KeyError(
                f"[BacktestL1] missing key in backtest.strategy: {e}"
            )

        with self.timed():
            logs.info(
                f"[BacktestL1] start data_version={ctx.date} "
                f"symbols={symbols} replay={replay_mode}"
            )

            # --------------------------------------------------
            # Symbol → Slice 自动解析（sh / sz）
            # --------------------------------------------------
            resolver = SymbolSliceResolver(
                meta_dir=ctx.meta_dir,
                stage="feature",
                output_slot="",
            )

            tables = resolver.get_many(symbols)

            handler = FeatureDataHandler.from_tables(
                tables=tables,
                feature_cols=strategy_cfg["features"],
                label_col=None,   # L1
            )

            streams = handler.iter_symbol_events()

            # --------------------------------------------------
            # Strategy / Portfolio / Engine
            # --------------------------------------------------
            strategy = ThresholdStrategy(
                feature=signal_feature,
                threshold=threshold,
            )

            portfolio = L1Portfolio()

            engine = BacktestEngine(
                strategy=strategy,
                portfolio=portfolio,
                execution=None,  # L1
            )

            replay = MultiSymbolReplay(streams)
            engine.run(replay.replay())

            # --------------------------------------------------
            # Output（按实验名 + date）
            # --------------------------------------------------
            out_dir = Path(ctx.backtest_dir)
            out_dir.mkdir(parents=True, exist_ok=True)

            out = out_dir / f"{self._bt.name}_{ctx.date}.txt"
            out.write_text(
                f"name={self._bt.name}\n"
                f"data_version={ctx.date}\n"
                f"symbols={symbols}\n"
                f"replay={replay_mode}\n"
                f"final_equity={portfolio.equity}\n"
                f"points={len(portfolio.equity_curve)}\n"
            )

            logs.info(
                f"[BacktestL1] done equity={portfolio.equity:.6f}"
            )
            return ctx
