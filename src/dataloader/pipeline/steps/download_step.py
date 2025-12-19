from __future__ import annotations

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.ftp_download_adapter import FtpDownloadAdapter
from src.engines.ftp_download_engine import FtpDownloadEngine
from src import logs


class DownloadStep(BasePipelineStep):
    """
    DownloadStep（冻结版）：

    - Step 决定是否下载
    - Engine 决定下载哪些文件
    - Adapter 负责执行
    """

    def __init__(
        self,
        adapter: FtpDownloadAdapter,
        engine: FtpDownloadEngine,
        inst=None,
    ):
        super().__init__(inst)
        self.adapter = adapter
        self.engine = engine

    def run(self, ctx: PipelineContext) -> PipelineContext:
        # --- 显式 skip 规则 ---
        logs.info(ctx.raw_dir)
        if list(ctx.raw_dir.glob("*.7z")):
            logs.info("[DownloadStep] raw/*.7z exists → skip download")
            return ctx

        date = self.engine.resolve_date(ctx.date)

        remote_files = self.adapter.list_remote_files(date)
        selected = self.engine.select_filenames(remote_files)

        if not selected:
            logs.warning("[DownloadStep] no files selected")
            return ctx

        self.adapter.download_files(
            date=date,
            filenames=selected,
            local_dir=ctx.raw_dir,
        )

        return ctx
