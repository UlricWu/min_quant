#!filepath: src/backtest/steps/alpha/build_minute_dataview_step.py
from __future__ import annotations

from src import logs
from src.backtest.context import BacktestContext
from src.backtest.engines.alpha.data_view import MinuteFeatureDataView
from src.meta.symbol_slice_resolver import SymbolSliceResolver
from src.pipeline.step import PipelineStep


class BuildMinuteDataViewStep(PipelineStep):
    """
    BuildMinuteDataViewStep（FINAL / FROZEN）

    Contract:
    - Build observable market world (MinuteFeatureDataView).
    - Feature schema MUST come from ctx.model_artifact (single source of truth).
    - Output: ctx.data_view
    """

    stage = "build_data_view"

    def run(self, ctx: BacktestContext) -> BacktestContext:
        if ctx.meta_dir is None:
            raise RuntimeError("[BuildMinuteDataViewStep] meta_dir is not set")
        if ctx.model_artifact is None:
            raise RuntimeError("[BuildMinuteDataViewStep] model_artifact is not resolved")

        logs.info(f"[BuildMinuteDataViewStep] date={ctx.today}")

        resolver = SymbolSliceResolver(
            meta_dir=ctx.meta_dir,
            stage="feature",
        )

        ctx.data_view = MinuteFeatureDataView(
            resolver=resolver,
            symbols=ctx.symbols,
            feature_names=ctx.model_artifact.feature_names,
            price_col="close",
        )
        return ctx
