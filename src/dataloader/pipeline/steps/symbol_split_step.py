from __future__ import annotations

from src import logs
from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.symbol_router_adapter import SymbolRouterAdapter


class SymbolSplitStep(BasePipelineStep):
    """
    Step = 控制流 + 时间语义边界
    """

    def __init__(self, adapter: SymbolRouterAdapter, inst=None):
        super().__init__(inst)
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        parquet_dir = ctx.parquet_dir
        symbol_dir = ctx.symbol_dir

        files = sorted(parquet_dir.glob("*.parquet"))
        if not files:
            logs.warning(f"[SymbolSplitStep] 无 parquet 文件: {parquet_dir}")
            return ctx

        # Step scope（record=False）
        with self.timed():
            self.adapter.split(
                date=ctx.date,
                parquet_files=files,
                symbol_dir=symbol_dir,
            )

        return ctx
