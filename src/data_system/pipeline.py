#!filepath: src/data_system/pipeline.py
from __future__ import annotations

from src.data_system.context import DataContext
from src.pipeline.step import PipelineStep
from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs
from src.observability.instrumentation import Instrumentation
from src.pipeline.pipeline import PipelineAbort


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
        fact_dir = self.pm.fact_dir(date)
        meta_dir = self.pm.meta_dir(date)
        feature_l0_dir = self.pm.feature_dir(date)
        label_dir = self.pm.label_dir(date)
        normalized_dir = self.pm.l2_normalized_dir(date)

        FileSystem.ensure_dir(raw_dir)
        FileSystem.ensure_dir(fact_dir)
        FileSystem.ensure_dir(meta_dir)
        FileSystem.ensure_dir(feature_l0_dir)
        FileSystem.ensure_dir(label_dir)
        FileSystem.ensure_dir(normalized_dir)


        ctx = DataContext(
            today=date,
            raw_dir=raw_dir,
            fact_dir=fact_dir,
            meta_dir=meta_dir,
            feature_dir=feature_l0_dir,
            label_dir=label_dir,
            normalized_dir=normalized_dir
        )

        # --------------------------------------------------
        # 核心循环：Pipeline 不打 timer
        # --------------------------------------------------
        try:
            for step in self.steps:
                ctx = step.run(ctx)
        except PipelineAbort as e:
            logs.info(f"[Pipeline][SKIP] {e}")
            self._cleanup_date_dirs(ctx)
            return ctx

        # Timeline 只包含 leaf（由 Step / Adapter 写入）
        self.inst.generate_timeline_report(date)

        return ctx

    def _cleanup_date_dirs(self, ctx: DataContext):
        """
        删除该 date 下所有可能被创建的目录
        失败日必须在文件系统中“不可见”
        """
        for path in (
                ctx.raw_dir,
                ctx.fact_dir,
                ctx.meta_dir,
                ctx.feature_dir,
                ctx.label_dir,
        ):
            try:
                if path.exists():
                    FileSystem.remove(path)
                    logs.warning(f"[Pipeline][CLEANUP] removed {path}")
            except Exception as e:
                logs.error(f"[Pipeline][CLEANUP][FAILED] {path} | {e}")
