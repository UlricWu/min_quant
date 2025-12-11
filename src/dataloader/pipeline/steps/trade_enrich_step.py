#!filepath: src/dataloader/pipeline/steps/trade_enrich_step.py
from __future__ import annotations

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src import logs


class TradeEnrichStep(PipelineStep):

    def __init__(self, adapter):
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logs.info("[Step] TradeEnrichStep")
        self.adapter.enrich_date(ctx.date)
        return ctx
