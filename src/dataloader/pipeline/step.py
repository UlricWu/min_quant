#!filepath: src/dataloader/pipeline/step.py
from __future__ import annotations

from typing import Protocol
from src.dataloader.pipeline.context import PipelineContext
from src.observability.instrumentation import Instrumentation

class PipelineStep(Protocol):
    """
    所有 Pipeline Step 的统一接口：

    - 输入：PipelineContext
    - 输出：PipelineContext（可以原样返回或写入附加信息）
    """

    def run(self, ctx: PipelineContext) -> PipelineContext:
        ...

class BasePipelineStep:
    """
    Step 基类（非必须）
    提供：
    - step_name 自动推断
    - 可选 Instrumentation
    """

    def __init__(self, inst: Instrumentation | None = None):
        self.inst = inst

    @property
    def step_name(self) -> str:
        return self.__class__.__name__

    def timed(self):
        """
        获取计时 context manager。
        如果 inst 为 None，则返回一个 dummy context manager。
        """
        if self.inst is None:
            return _DummyTimer()
        return self.inst.timer(self.step_name)


class _DummyTimer:
    """Instrumentation 为 None 时的无操作计时器。"""
    def __enter__(self): pass
    def __exit__(self, exc_type, exc, tb): pass