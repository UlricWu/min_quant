#!filepath: src/dataloader/pipeline/steps/normalize_step.py
from __future__ import annotations

from pathlib import Path

from py7zr.helpers import canonical_path

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.normalize_adapter import NormalizeAdapter
from src import logs
from src.utils.filesystem import FileSystem


class NormalizeStep(BasePipelineStep):
    def __init__(self, *, adapter: NormalizeAdapter, skip_if_exists: bool = True, inst=None):
        super().__init__(inst)
        self.adapter = adapter
        self.skip_if_exists = skip_if_exists

    def run(self, ctx: PipelineContext) -> PipelineContext:
        parquet_dir: Path = ctx.parquet_dir
        date: str = ctx.date

        canonical_dir: Path = ctx.canonical_dir

        if self.skip_if_exists and self._already_normalized(canonical_dir):
            logs.info(f"[NormalizeStep] skip normalize date={date}")
            return ctx
        logs.info(f"[NormalizeStep] normalize date={date} in parquet_dir={parquet_dir} output_dir={canonical_dir}")

        with self.timed():
            self.adapter.run(
                date=date,
                input_dir=parquet_dir,  # /data/parquet/<date>
                output_dir=canonical_dir  # /data/canonical/<date>
            )

        return ctx

    @staticmethod
    def _already_normalized(canonical_dir: Path) -> bool:
        if not canonical_dir.exists():
            return False

        return any(
            canonical_dir.glob("*_Trade.parquet")
        ) or any(
            canonical_dir.glob("*_Order.parquet")
        )
