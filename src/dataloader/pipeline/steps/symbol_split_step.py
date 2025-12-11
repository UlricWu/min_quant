#!filepath: src/dataloader/pipeline/steps/symbol_split_step.py
from __future__ import annotations

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.adapters.symbol_split_adapter import SymbolSplitAdapter
from src import logs


class SymbolSplitStep(PipelineStep):
    """
    Step 3: parquet/<date> → data/symbol/<symbol>/<date>/{Order,Trade}.parquet
    """

    def __init__(self, adapter: SymbolSplitAdapter):
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logs.info("[SymbolSplitStep] 开始 Symbol 拆分")
        self.adapter.split_date(ctx.date)
        return ctx
