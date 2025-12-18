#!filepath: src/dataloader/pipeline/steps/trade_enrich_step.py
from __future__ import annotations

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.trade_enrich_adapter import TradeEnrichAdapter


class TradeEnrichStep(BasePipelineStep):
    """
    TradeEnrich Step（最终版）

    语义：
    - Step = 父级时间语义边界（进入 timeline）
    """

    def __init__(
        self,
        adapter: TradeEnrichAdapter,
        inst=None,
    ):
        super().__init__(inst)
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        # Step 级 timer（record=True）
        with self.inst.timer("TradeEnrich"):
            self.adapter.run(
                date=ctx.date,
                symbol_root=ctx.symbol_dir,
            )
        return ctx
