#!filepath: src/dataloader/pipeline/steps/orderbook_rebuild_step.py
from __future__ import annotations

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.orderbook_rebuild_adapter import OrderBookRebuildAdapter
from src import logs


class OrderBookRebuildStep(BasePipelineStep):
    """
    OrderBook Rebuild Step（最终版）

    语义：
    - Step = 父级时间语义边界（record=True）
    - Adapter = 具体执行
    """

    def __init__(
        self,
        adapter: OrderBookRebuildAdapter,
        inst=None,
    ):
        super().__init__(inst)
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        date = ctx.date
        symbol_root = ctx.symbol_dir

        # Step 级 timer（进入 timeline）
        with self.inst.timer("OrderBookRebuild"):
            self.adapter.run(
                date=date,
                symbol_root=symbol_root,
            )

        return ctx
