from __future__ import annotations

from debugpy.common.util import force_str

from src import logs
from src.meta.base import BaseMeta, MetaOutput
from src.pipeline.context import PipelineContext
from src.observability.instrumentation import (
    Instrumentation,
    NoOpInstrumentation,
)


class PipelineStep:
    """
    Pipeline Step 基类（最终冻结版）

    职责（唯一）：
      1. 作为 orchestration 层（循环 / 调度 / 条件执行）
      2. 提供 Step 级时间语义边界（parent scope）

    设计铁律：
      - Step 本身不进入 timeline
      - 所有可观测性能必须发生在 Step 内部（leaf timer）
      - Instrumentation 是可选横切关注点
      - Step 行为不依赖 inst 是否存在

    """

    stage: str = ''  # e.g. "normalize"
    upstream_stage: str = ''  # e.g. "parquet"
    output_slot: str
    def __init__(self, inst: Instrumentation | None = None):
        # 永远保证 inst 可用（No-op 语义）
        self.inst: Instrumentation | NoOpInstrumentation = (
            inst if inst is not None else NoOpInstrumentation()
        )

    # --------------------------------------------------
    # Step identity
    # --------------------------------------------------
    @property
    def step_name(self) -> str:
        """默认使用类名作为 Step 名称。"""
        return self.__class__.__name__

    # --------------------------------------------------
    # Step-level timer（parent scope, not recorded）
    # --------------------------------------------------
    def timed(self):
        """
        Step 级时间语义边界（父级 scope）

        - record=False
        - 不进入 timeline
        - 仅用于包裹 Step.run 的 wall-time
        """
        return self.inst.timer(self.step_name, record=False)

    # --------------------------------------------------
    # Contract
    # --------------------------------------------------

    def run(self, ctx: PipelineContext) -> PipelineContext:
        """
        子类必须实现。
        """
        # raise NotImplementedError
        pass

    def _run(self, ctx: PipelineContext) -> PipelineContext:
        meta = BaseMeta(meta_dir=ctx.meta_dir, stage=self.stage, output_slot=self.output_slot)

        for file in ctx.last_stage:
            exchange = file.stem

            if not meta.upstream_changed(file):
                logs.warning(f"[{self.step_name}] {exchange} unchanged -> skip")
                continue

            meta.commit(
                MetaOutput(
                    input_file=file,
                    output_file=file,
                    rows=0,  # CsvConvert 阶段不关心 rows
                )
            )

            logs.info(
                f"[{self.step_name}] meta committed for {file.name}"
            )

        return ctx
