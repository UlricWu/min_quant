#!filepath: src/dataloader/pipeline/pipeline.py
from __future__ import annotations
from typing import List

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs


class DataPipeline:
    """
    Step-based Pipeline：
    - 自身不做业务逻辑
    - 只负责构建 context + 依次调用 steps
    """

    def __init__(self, steps: List[PipelineStep], path_manager: PathManager):
        self.steps = steps
        self.pm = path_manager

    def run(self, date: str) -> PipelineContext:
        logs.info(f"[DataPipeline] ====== START {date} ======")

        raw_dir = self.pm.raw_dir(date)
        parquet_dir = self.pm.parquet_dir(date)
        symbol_root = raw_dir /'symbol'
        #
        FileSystem.ensure_dir(raw_dir)
        FileSystem.ensure_dir(parquet_dir)
        FileSystem.ensure_dir(symbol_root)

        ctx = PipelineContext(
            date=date,
            raw_dir=raw_dir,
            parquet_dir=parquet_dir,
            symbol_root=symbol_root,
        )

        for step in self.steps:
            ctx = step.run(ctx)

        logs.info(f"[DataPipeline] ====== DONE {date} ======")
        return ctx
