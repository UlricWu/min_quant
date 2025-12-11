#!filepath: src/dataloader/pipeline/steps/symbol_split_step.py
from __future__ import annotations

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src import logs


class SymbolSplitStep(PipelineStep):

    def __init__(self, router):
        self.router = router

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logs.info("[Step] SymbolSplitStep")
        self.router.route_date(ctx.date)
        return ctx
