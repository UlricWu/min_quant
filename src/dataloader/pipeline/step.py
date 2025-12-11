#!filepath: src/dataloader/pipeline/step.py
from __future__ import annotations
from typing import Protocol
from src.dataloader.pipeline.context import PipelineContext


class PipelineStep(Protocol):
    def run(self, ctx: PipelineContext) -> PipelineContext:
        ...
