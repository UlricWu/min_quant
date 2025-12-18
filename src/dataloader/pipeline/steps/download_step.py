from __future__ import annotations
from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.ftp_download_adapter import FtpDownloadAdapter

from src.utils.logger import logs


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
        seven_z_files = list(ctx.raw_dir.glob("*.7z"))

        if seven_z_files:
            logs.info("[Pipeline] raw/*.7z 已存在 → 跳过下载")
            return ctx

        with self.timed():
            self.adapter.download_date(ctx.date, ctx.raw_dir)
        return ctx
