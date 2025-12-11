#!filepath: src/dataloader/pipeline/pipeline.py
from __future__ import annotations
from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs


class DataPipeline:

    def __init__(self, steps: list[PipelineStep], pm: PathManager):
        self.steps = steps
        self.pm = pm

    def run(self, date: str):
        logs.info(f"[Pipeline] ====== START {date} ======")

        raw_dir = self.pm.raw_dir(date)
        parquet_dir = self.pm.parquet_dir(date)
        symbol_dir = raw_dir / 'symbol'

        FileSystem.ensure_dir(raw_dir)
        FileSystem.ensure_dir(parquet_dir)
        FileSystem.ensure_dir(symbol_dir)

        ctx = PipelineContext(
            date=date,
            raw_dir=raw_dir,
            parquet_dir=parquet_dir,
            symbol_dir=symbol_dir,
        )

        for step in self.steps:
            ctx = step.run(ctx)

        logs.info(f"[Pipeline] ====== DONE {date} ======")
        return ctx
