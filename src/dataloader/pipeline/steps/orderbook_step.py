#!filepath: src/dataloader/pipeline/steps/orderbook_step.py
from __future__ import annotations

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src import logs


class OrderBookStep(PipelineStep):

    def __init__(self, adapter, symbols):
        self.adapter = adapter
        self.symbols = [int(s) for s in symbols]

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logs.info("[Step] OrderBookStep")
        for sym in self.symbols:
            self.adapter.build_symbol_orderbook(sym, ctx.date)
        return ctx
