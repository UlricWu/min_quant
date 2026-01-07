from __future__ import annotations

from src.observability.instrumentation import (
    Instrumentation,
    NoOpInstrumentation,
)
from src.pipeline.context import BaseContext

from abc import ABC, abstractmethod
class PipelineStep(ABC):
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
    @abstractmethod
    def run(self, ctx):
        """
        Execute this step.

        Parameters:
        - ctx: system-specific context

        Returns:
        - ctx (mutated or replaced)
        """
        raise NotImplementedError