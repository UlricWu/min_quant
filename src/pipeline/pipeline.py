# src/pipeline/pipeline.py
from __future__ import annotations
from typing import List

from src.pipeline.step import PipelineStep


class PipelineAbort(Exception):
    """
    PipelineAbort (FINAL)
        用于 Step 主动中断 Pipeline 的信号异常

    某个 Step 有权声明：
        “在当前上下文下，Pipeline 不应继续执行。

    Used by steps to signal controlled early termination.
    """
    pass


class BasePipeline:
    """
    BasePipeline (FINAL / FROZEN)

    Orchestrates ordered execution of steps.

    Design invariants:
    - Owns execution order only
    - Does not inspect context internals
    - Does not perform business logic
    """

    def __init__(self, *, steps: List[PipelineStep]):
        self.steps = steps

    def run(self, ctx):
        for step in self.steps:
            try:
                ctx = step.run(ctx)
            except PipelineAbort:
                break
        return ctx
