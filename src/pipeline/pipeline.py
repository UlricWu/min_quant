#!filepath: src/pipeline/pipeline.py
from __future__ import annotations

from src.pipeline.context import PipelineContext
from src.pipeline.step import PipelineStep
from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs
from src.observability.instrumentation import Instrumentation

class DataPipeline:
    """
    DataPipeline = 调度器（Scheduler）

    设计铁律：
    - Pipeline 负责 orchestration（顺序 / 上下文）
    - Pipeline 不负责任何 Step 级计时
    - Step 自己定义时间语义边界（via BasePipelineStep.timed）
    """

    def __init__(
            self,
            steps: list[PipelineStep],
            pm: PathManager,
            inst: Instrumentation,
    ):
        self.steps = steps
        self.pm = pm
        self.inst = inst

    def run(self, date: str):
        logs.info(f"[Pipeline] ====== START {date} ======")

        raw_dir = self.pm.raw_dir(date)
        parquet_dir = self.pm.parquet_dir(date)
        symbol_dir = self.pm.symbol_dir(date)
        normalize_dir = self.pm.canonical_dir(date)
        meta_dir = self.pm.meta_dir(date)

        FileSystem.ensure_dir(raw_dir)
        FileSystem.ensure_dir(parquet_dir)
        FileSystem.ensure_dir(symbol_dir)
        FileSystem.ensure_dir(normalize_dir)
        FileSystem.ensure_dir(meta_dir)

        ctx = PipelineContext(
            date=date,
            raw_dir=raw_dir,
            parquet_dir=parquet_dir,
            canonical_dir=normalize_dir,
            symbol_dir=symbol_dir,
            meta_dir=meta_dir,
        )

        # --------------------------------------------------
        # 核心循环：Pipeline 不打 timer
        # --------------------------------------------------
        for step in self.steps:
            ctx = step.run(ctx)

        # Timeline 只包含 leaf（由 Step / Adapter 写入）
        self.inst.generate_timeline_report(date)

        return ctx
