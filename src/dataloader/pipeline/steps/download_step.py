#!filepath: src/dataloader/pipeline/steps/download_step.py
from __future__ import annotations
from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src import logs


class DownloadStep(PipelineStep):

    def __init__(self, downloader):
        self.downloader = downloader

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logs.info("[Step] DownloadStep")
        files = list(ctx.raw_dir.glob("*.7z"))
        if not files:
            self.downloader.download(ctx.date)
        return ctx
