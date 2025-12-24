# src/dataloader/pipeline/step.py
from __future__ import annotations

from pathlib import Path
from typing import Protocol

from src import logs
from src.pipeline.context import PipelineContext
from src.observability.instrumentation import Instrumentation, NoOpInstrumentation


class PipelineStep(Protocol):
    """
        Pipeline Step 基类（冻结版）

        职责（唯一）：
          1. 作为 orchestration 层（循环 / 调度 / 条件执行）
          2. 提供 Step 级时间语义边界（parent scope）

        设计铁律：
          - Step 本身不进入 timeline
          - 所有可观测性能必须发生在 Step 内部（leaf timer）
          - Step 不再负责 skip / output / cache 语义
        """

    def __init__(self, inst: Instrumentation | None = None):
        self.inst: Instrumentation | NoOpInstrumentation = (
            inst if inst is not None else NoOpInstrumentation()
        )

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

