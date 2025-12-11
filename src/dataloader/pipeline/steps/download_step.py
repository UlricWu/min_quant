#!filepath: src/dataloader/pipeline/steps/download_step.py
from __future__ import annotations
from pathlib import Path

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.dataloader.ftp_downloader import FTPDownloader
from src import logs


class DownloadStep(PipelineStep):
    """
    Step 1: raw/*.7z 下载（若已有则跳过）
    """

    def __init__(self, downloader: FTPDownloader):
        self.downloader = downloader

    def run(self, ctx: PipelineContext) -> PipelineContext:
        raw_dir: Path = ctx.raw_dir
        seven_z_files = list(raw_dir.glob("*.7z"))

        if not seven_z_files:
            logs.info(f"[DownloadStep] {raw_dir} 无 .7z → 开始 FTP 下载")
            self.downloader.download(ctx.date)
            seven_z_files = list(raw_dir.glob("*.7z"))
        else:
            logs.info(f"[DownloadStep] {raw_dir} 已有 .7z → 跳过下载")

        ctx.set_artifact("seven_z_files", seven_z_files)
        return ctx
