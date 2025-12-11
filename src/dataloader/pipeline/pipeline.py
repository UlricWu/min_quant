#!filepath: src/dataloader/pipeline/pipeline.py
from __future__ import annotations
from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs
from src.observability.instrumentation import Instrumentation


class DataPipeline:

    def __init__(self, steps: list[PipelineStep], pm: PathManager, inst: Instrumentation):
        self.steps = steps
        self.pm = pm
        self.inst = inst

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
            # 自动获取 Step 名称
            step_name = step.__class__.__name__

            # 对每一个 Step 做计时记录
            with self.inst.timer(step_name):
                # Step 需要接收 inst，否则内部 Adapter 用不到
                ctx = step.run(ctx)

        self.inst.generate_timeline_report(date)

        logs.info(f"[Pipeline] ====== DONE {date} ======")
        return ctx
