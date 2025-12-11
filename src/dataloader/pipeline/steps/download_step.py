from __future__ import annotations
from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.ftp_download_adapter import FtpDownloadAdapter


class DownloadStep(BasePipelineStep):
    """
    Pipeline Step：
    - 调用 DownloadAdapter 进行下载
    - Step 不关心 FTP 逻辑
    """

    def __init__(self, adapter: FtpDownloadAdapter, inst=None):
        super().__init__(inst)
        self.adapter = adapter

    def run(self, ctx: PipelineContext) -> PipelineContext:
        with self.timed():
            self.adapter.download_date(ctx.date, ctx.raw_dir)
        return ctx
