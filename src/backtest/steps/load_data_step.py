# src/backtest/steps/load_data_step.py
from pathlib import Path
from src.pipeline.step import PipelineStep
from src import logs

from src.meta.symbol_slice_resolver import SymbolSliceResolver
from src.backtest.data.feature_data_handler import FeatureDataHandler


class LoadDataStep(PipelineStep):
    """
    LoadDataStep（FINAL）

    职责：
      - symbols → Arrow tables
      - 构造 FeatureDataHandler
    """

    stage = "backtest_load"
    output_slot = "data"

    def __init__(self, *, backtest_cfg, inst=None):
        super().__init__(inst=inst)
        self._bt = backtest_cfg

    def run(self, ctx):
        symbols = self._bt.symbols
        strategy_cfg = self._bt.strategy
        feature_cols = strategy_cfg["features"]

        with self.timed():
            logs.info(f"[LoadData] symbols={symbols}")

            resolver = SymbolSliceResolver(
                meta_dir=ctx.meta_dir,
                stage="feature",
                output_slot="",
            )

            tables = resolver.get_many(symbols)

            handler = FeatureDataHandler.from_tables(
                tables=tables,
                feature_cols=feature_cols,
                label_col=None,  # L1
            )

            ctx.tables = tables
            ctx.data_handler = handler

            return ctx
