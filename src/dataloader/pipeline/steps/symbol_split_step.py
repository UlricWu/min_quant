#!filepath: src/dataloader/pipeline/steps/symbol_split_step.py
from __future__ import annotations

from src import logs

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.symbol_router_adapter import SymbolRouterAdapter

from src.utils.path import PathManager


class SymbolSplitStep(BasePipelineStep):

    def __init__(self, adapter: SymbolRouterAdapter, path_manager=PathManager, symbols=None, inst=None):
        super().__init__(inst)
        if symbols is None:
            symbols = []
        self.adapter = adapter
        self.path_manager = path_manager
        self.symbols = symbols
        self.parquet_dir = self.path_manager.parquet_dir()

    def run(self, ctx: PipelineContext) -> PipelineContext:

        files = sorted(self.parquet_dir.glob("*.parquet"))
        if not files:
            logs.warning(f"[SymbolRouter] 无 parquet 文件: {self.parquet_dir}")
            return ctx

        with self.timed():
            self.adapter.split(ctx.date)
        return ctx
