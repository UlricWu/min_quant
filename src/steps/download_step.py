# src/steps/download_step.py
from __future__ import annotations

import ftplib
import json
import subprocess
from pathlib import Path
from typing import Optional

from src.pipeline.context import PipelineContext
from src.engines.ftp_download_engine import FtpDownloadEngine
from src.utils.filesystem import FileSystem
from src.utils.retry import Retry
from src.utils.logger import logs
from src.config.secret_config import SecretConfig
from src.config.pipeline_config import DownloadBackend
from src.pipeline.pipeline import PipelineAbort
from src.meta.base import MetaOutput
from src.pipeline.step import PipelineStep


class DownloadStep(PipelineStep):
    """
    DownloadStep (Source Step / FINAL)

    Semantics:
      upstream : virtual remote file (ftp://...)
      output   : local raw/*.7z
      rows     : 0

    Error policy:
      - Non-fatal (no data / permission) -> PipelineAbort
      - Fatal (network / bug)            -> raise
    """

    stage = "download"
    upstream_stage = "remote"

    # --------------------------------------------------
    def __init__(
        self,
        *,
        engine: FtpDownloadEngine,
        secret: SecretConfig,
        backend: DownloadBackend = DownloadBackend.CURL,
        inst=None,
        remote_root: str = "",
    ) -> None:
        super().__init__(inst=inst)

        self.backend = backend
        self.remote_root = remote_root.rstrip("/")

        self.host = secret.ftp_host
        self.user = secret.ftp_user
        self.password = secret.ftp_password
        self.port = secret.ftp_port

    # ==================================================
    # Source Step: inject upstream logical items
    # ==================================================
    def run(self, ctx: PipelineContext) -> PipelineContext:
        """
        Source Step override:
          - enumerate remote plans
          - publish ctx.remote_outputs
          - then delegate to BasePipelineStep.run
        """

        ctx.last_stage = [Path('~/data/raw/2015-01-01/SZ_Order.csv.7z')]
        return ctx


    #     date_str = self.engine.resolve_date(ctx.date)
    #
    #     ftp: Optional[ftplib.FTP] = None
    #     try:
    #         ftp = ftplib.FTP(timeout=60)
    #         ftp.connect(self.host, self.port)
    #         ftp.login(self.user, self.password)
    #         ftp.cwd(self.remote_root)
    #         ftp.cwd(date_str)
    #
    #         filenames = ftp.nlst()
    #w
    #         plans = self.engine.plan_downloads(
    #             date=date_str,
    #             available_files=filenames,
    #         )
    #
    #         # 👇 关键：发布 Source outputs
    #         ctx.remote_outputs = plans
    #
    #     except Exception as e:
    #         msg = str(e)
    #         if self._is_non_fatal_error(msg):
    #             logs.warning(
    #                 f"[DownloadStep][SKIP] server issue -> skip pipeline | {msg}"
    #             )
    #             raise PipelineAbort(msg)
    #         raise
    #
    #     finally:
    #         if ftp is not None:
    #             try:
    #                 ftp.quit()
    #             except Exception:
    #                 pass
    #
    #     # 交回模板执行
    #     return super().run(ctx)
    #
    # # ==================================================
    # # BasePipelineStep hooks
    # # ==================================================
    # def resolve_input(self, ctx: PipelineContext, item: dict) -> Path:
    #     """
    #     Virtual upstream path for Meta.
    #     """
    #     date = self.engine.resolve_date(ctx.date)
    #     filename = item["filename"]
    #
    #     return Path(
    #         f"ftp://{self.host}:{self.port}"
    #         f"/{self.remote_root}/{date}/{filename}"
    #     )
    #
    # def execute(self, *, item: dict, input_file: Path):
    #     """
    #     Perform download.
    #     """
    #     filename = item["filename"]
    #     local_path = ctx.raw_dir / filename  # NOTE: ctx not allowed here
    #
    #     raise RuntimeError(
    #         "execute() should never be called directly; "
    #         "Download happens in write()"
    #     )
    #
    # def write(self, *, ctx: PipelineContext, item: dict, result) -> Path:
    #     """
    #     Download and return local file path.
    #     """
    #     filename = item["filename"]
    #     local_path = ctx.raw_dir / filename
    #
    #     upstream = self.resolve_input(ctx, item)
    #
    #     # meta hit handled by BasePipelineStep
    #
    #     with self.inst.timer(f"download_{filename}"):
    #         self._download_one(
    #             date=self.engine.resolve_date(ctx.date),
    #             plan=item,
    #             local_path=local_path,
    #         )
    #
    #     return local_path
    #
    # def logical_key(self, *, item, result):
    #     """
    #     Downstream logical key = filename
    #     """
    #     return item["filename"]
    #
    # # ==================================================
    # # Download helpers (unchanged)
    # # ==================================================
    # def _download_one(self, *, date: str, plan: dict, local_path: Path) -> None:
    #     if self.backend == DownloadBackend.CURL:
    #         self._download_by_curl(date, plan["filename"], local_path)
    #     else:
    #         self._download_by_ftplib(plan["filename"], local_path)
    #
    # @Retry.decorator(
    #     exceptions=(ftplib.error_temp, ftplib.error_perm, OSError, TimeoutError),
    #     max_attempts=2,
    #     delay=30,
    #     backoff=2,
    #     jitter=True,
    # )
    # def _download_by_ftplib(self, remote_file: str, local_path: Path) -> None:
    #     logs.info(f"[FTP] ftplib downloading {remote_file}")
    #     FileSystem.ensure_dir(local_path.parent)
    #
    #     with ftplib.FTP(timeout=60) as ftp:
    #         ftp.connect(self.host, self.port)
    #         ftp.login(self.user, self.password)
    #         ftp.cwd(self.remote_root)
    #         ftp.retrbinary(f"RETR {remote_file}", local_path.open("wb").write)
    #
    # @Retry.decorator(
    #     exceptions=(RuntimeError, OSError),
    #     max_attempts=3,
    #     delay=30,
    #     backoff=2,
    #     jitter=True,
    # )
    # def _download_by_curl(
    #     self, date: str, remote_file: str, local_path: Path
    # ) -> None:
    #     logs.info(f"[FTP] curl downloading {remote_file}")
    #     FileSystem.ensure_dir(local_path.parent)
    #
    #     url = (
    #         f"ftp://{self.host}:{self.port}"
    #         f"/{self.remote_root}/{date}/{remote_file}"
    #     )
    #
    #     cmd = [
    #         "curl",
    #         "--noproxy", "*",
    #         "--ftp-pasv",
    #         "-u", f"{self.user}:{self.password}",
    #         "--fail",
    #         "--connect-timeout", "30",
    #         "--retry", "5",
    #         "--retry-delay", "10",
    #         "--silent",
    #         "--show-error",
    #         "--stderr", "-",
    #         "--write-out",
    #         "{\"speed_bps\": %{speed_download}, \"size_bytes\": %{size_download}}\n",
    #         "-o", str(local_path),
    #         url,
    #     ]
    #
    #     result = subprocess.run(
    #         cmd,
    #         stdout=subprocess.PIPE,
    #         stderr=subprocess.PIPE,
    #         text=True,
    #         timeout=600,
    #     )
    #
    #     if result.stdout.strip():
    #         stats = json.loads(result.stdout)
    #         logs.info(
    #             f"[DownloadStats] {remote_file} | "
    #             f"speed={stats['speed_bps'] * 8 / 1_000_000:.2f} Mbps "
    #             f"size={stats['size_bytes'] / 1_000_000_000:.2f} GB"
    #         )
    #
    #     logs.info(f"[Download] finished {remote_file} -> {local_path}")
    #
    # # ==================================================
    # @staticmethod
    # def _is_non_fatal_error(msg: str) -> bool:
    #     msg = msg.lower()
    #     return any(
    #         k in msg
    #         for k in (
    #             "trading",
    #             "no data",
    #             "no such file",
    #             "not found",
    #             "550",
    #             "permission",
    #             "failed to change directory",
    #         )
    #     )
