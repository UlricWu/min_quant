#!filepath: src/dataloader/pipeline/steps/symbol_split_step.py
from __future__ import annotations

from src import logs, FileSystem
from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.symbol_router_adapter import SymbolRouterAdapter


class SymbolSplitStep(BasePipelineStep):
    """
    Symbol 拆分 Step（最终版）：

    语义：
    - Step 是父级时间语义边界（record=False）
    - 实际 IO / 拆分由 Adapter 完成
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

        # ✅ Step 层准备运行环境
        FileSystem.ensure_dir(symbol_dir)
        # Step 级 scope（不进 timeline）
        with self.timed():
            self.adapter.split(
                date=ctx.date,
                parquet_files=files,
                symbol_dir=symbol_dir,
            )

        return ctx
