#!filepath: src/dataloader/pipeline/steps/trade_enrich_step.py
from __future__ import annotations

from pathlib import Path

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
from src import logs


class TradeEnrichStep(BasePipelineStep):
    """
    TradeEnrich Pipeline Step（trade-only）

    输入：
        Events.parquet（normalize 后）

    输出：
        Trade.parquet（只包含成交 + 增强字段）
    """

    name = "TradeEnrichStep"

    def __init__(
        self,
        adapter: TradeEnrichAdapter,
        inst=None,
    ):
        super().__init__(inst)
        self.adapter = adapter

    # # --------------------------------------------------
    # # ⭐ 告诉 pipeline：本 Step 的主输出文件
    # # --------------------------------------------------
    # def output_path(self, ctx: PipelineContext) -> Path:
    #     return ctx.parquet_dir / ".TradeEnrich.done"

    # --------------------------------------------------
    # 核心执行逻辑
    # --------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        # # ⭐ 关键：存在即跳过
        # if self.should_skip(ctx):
        #     return ctx

        # Step 级 timer（record=True）
        with self.inst.timer("TradeEnrich"):
            self.adapter.run(
                date=ctx.date,
                symbol_root=ctx.symbol_dir,
            )

        # # ⭐ 标记 Step 已完成
        # done = self.output_path(ctx)
        # done.parent.mkdir(parents=True, exist_ok=True)
        # done.touch()

        return ctx
