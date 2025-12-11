#!filepath: src/dataloader/pipeline/steps/symbol_split_step.py
from __future__ import annotations

from src import logs


from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.symbol_router_adapter import SymbolRouterAdapter


class SymbolSplitStep(BasePipelineStep):

    def __init__(self, adapter:SymbolRouterAdapter, inst=None):
        super().__init__(inst)
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        with self.timed():
            self.adapter.split(ctx.date, ctx.parquet_dir, ctx.symbol_dir)
        return ctx
