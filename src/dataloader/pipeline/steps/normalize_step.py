#!filepath: src/dataloader/pipeline/steps/normalize_step.py
from __future__ import annotations

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.normalize_adapter import NormalizeAdapter


class NormalizeStep(BasePipelineStep):
    """
    Normalize Step（最终版）

    语义：
    - Step = 事件标准化阶段
    """

    def __init__(self, adapter: NormalizeAdapter, inst=None):
        super().__init__(inst)
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        with self.inst.timer("Normalize"):
            self.adapter.run(
                date=ctx.date,
                symbol_root=ctx.symbol_dir,
            )
        return ctx
