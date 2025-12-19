#!filepath: src/dataloader/pipeline/steps/normalize_step.py
from __future__ import annotations

from pathlib import Path

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.normalize_adapter import NormalizeAdapter
from src import logs


class NormalizeStep(BasePipelineStep):
    def __init__(self, *, adapter: NormalizeAdapter, skip_if_exists: bool = True, inst=None):
        super().__init__(inst)
        self.adapter = adapter
        self.skip_if_exists = skip_if_exists

    def run(self, ctx: PipelineContext) -> PipelineContext:
        symbol_dir: Path = ctx.symbol_dir
        date: str = ctx.date

        if self.skip_if_exists and self._already_normalized(symbol_dir, date):
            logs.info(f"[NormalizeStep] skip normalize date={date}")
            return ctx

        with self.timed():
            self.adapter.run(date=date, symbol_dir=symbol_dir)

        return ctx

    @staticmethod
    def _already_normalized(symbol_dir: Path, date: str) -> bool:
        return any(symbol_dir.glob(f"*/{date}/order/Normalized.parquet"))
