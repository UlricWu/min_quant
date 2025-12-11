#!filepath: src/dataloader/pipeline/steps/csv_to_parquet_step.py
from __future__ import annotations

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.adapters.l2_raw_to_parquet_adapter import L2RawToParquetAdapter
from src import logs


class CsvToParquetStep(PipelineStep):
    """
    Step 2: raw/*.7z → parquet/<date>/SH_*.parquet / SZ_*.parquet
    """

    def __init__(self, adapter: L2RawToParquetAdapter):
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logs.info("[CsvToParquetStep] 开始 CSV→Parquet 转换")
        self.adapter.ensure_parquet_for_date(
            date=ctx.date,
            raw_dir=ctx.raw_dir,
            parquet_dir=ctx.parquet_dir,
        )
        return ctx
