#!filepath: src/dataloader/pipeline/steps/trade_enrich_step.py
from __future__ import annotations

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
from src import logs
from src.utils.filesystem import FileSystem


class TradeEnrichStep(PipelineStep):
    """
    Workflow 层 Step：

    - 本身不写任何业务逻辑
    - 只负责调用 Adapter（Adapter 再去调用 Engine）
    """

    def __init__(self, adapter: TradeEnrichAdapter, inst=None) -> None:
        super().__init__(inst)
        self.adapter = adapter

    def run(self, ctx: PipelineContext, inst=None) -> PipelineContext:
        logs.info(f"[Step] TradeEnrichStep date={ctx.date}")

        # Step 层准备运行环境
        FileSystem.ensure_dir(ctx.symbol_dir)

        self.adapter.run_for_date(ctx.date, symbol_dir=ctx.symbol_dir)
        return ctx
