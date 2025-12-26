from __future__ import annotations

import ftplib
import json
import subprocess
from pathlib import Path
from typing import Optional

from src.utils.filesystem import FileSystem
from src.utils.retry import Retry
from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.engines.ftp_download_engine import FtpDownloadEngine
from src.utils.logger import logs
from src.config.secret_config import SecretConfig
from src.config.pipeline_config import DownloadBackend

from src.pipeline.pipeline import PipelineAbort
from src.meta.meta import BaseMeta, MetaResult


class DownloadStep(PipelineStep):
    """
    DownloadStep（I/O 层，工程冻结版）

DownloadStep（Source Step / 冻结版）

    语义：
      - upstream：远端 FTP 文件（虚拟 Path，仅用于 Meta）
      - output  ：本地 raw/*.7z 文件
      - rows    ：0（Source Step，无表结构）

    行为约定：
      - 非程序错误（无交易 / 无数据 / 权限） → PipelineAbort（跳过）
      - 程序错误（网络 / curl / bug）      → 原样抛异常
    """

    def __init__(
            self,
            engine: FtpDownloadEngine,
            secret: SecretConfig,
            backend: DownloadBackend = DownloadBackend.CURL,
            inst=None,
            remote_root: str = "",
    ) -> None:
        super().__init__(inst)

        self.engine = engine
        self.backend = backend

        self._secret = secret
        self.host = secret.ftp_host
        self.user = secret.ftp_user
        self.password = secret.ftp_password
        self.port = secret.ftp_port

        self.remote_root = remote_root.rstrip("/")

    # ======================================================
    def run(self, ctx: PipelineContext) -> PipelineContext:
        local_dir = ctx.raw_dir

        # # 已有 7z → 跳过
        # if list(local_dir.glob("*.7z")):
        #     logs.warning("[DownloadStep] raw/*.7z 已存在 → skip")
        #     return ctx
        #
        date_str = self.engine.resolve_date(ctx.date)

        # --------------------------------------------------
        # Download Meta（每个文件一个 manifest）
        # --------------------------------------------------
        meta = BaseMeta(ctx.meta_dir, stage="download")

        ftp: Optional[ftplib.FTP] = None

        try:
            # --------------------------------------------------
            # 1. FTP 仅用于列目录（即使 backend=curl）
            # --------------------------------------------------
            ftp = ftplib.FTP(timeout=60)
            ftp.connect(self.host, self.port)
            ftp.login(self.user, self.password)
            ftp.cwd(self.remote_root)
            ftp.cwd(date_str)

            filenames = ftp.nlst()

            plans = self.engine.plan_downloads(
                date=date_str,
                available_files=filenames,
            )

            # --------------------------------------------------
            # 2. 执行下载
            # --------------------------------------------------
            for plan in plans:
                filename = plan["filename"]
                local_path = local_dir / filename

                upstream = self._remote_upstream_path(date_str, filename)

                # ---------- Meta 命中：已完成 ----------
                if local_path.exists() and not meta.upstream_changed(upstream):
                    logs.info(
                        f"[DownloadStep] meta hit → skip {filename}"
                    )
                    continue

                with self.inst.timer(f'downloading_{filename}'):

                    self._download_one(
                        ftp=ftp,
                        date=date_str,
                        plan=plan,
                        local_path=local_path,
                    )

                # ---------- Meta Commit（仅成功后） ----------
                result = MetaResult(
                    input_file=upstream,
                    output_file=local_path,
                    rows=0,
                )
                meta.commit(result)
        except Exception as e:
            msg = str(e)
            if self._is_non_fatal_error(msg):
                logs.warning(
                    f"[DownloadStep][SKIP] server issue → skip pipeline | {msg}"
                )
                # 关键：用于终止 pipeline，但不是错误
                raise PipelineAbort(msg)

                # 关键：其它异常必须继续抛
            raise
        finally:
            if ftp is not None:
                try:
                    ftp.quit()
                except Exception:
                    pass

        return ctx

    # ======================================================
    # Virtual upstream (Source Step)
    # ======================================================
    def _remote_upstream_path(self, date: str, filename: str) -> Path:
        """
        虚拟 upstream，仅用于 Meta：
        ftp://host:port/root/date/filename
        """
        return Path(
            f"ftp://{self.host}:{self.port}"
            f"/{self.remote_root}/{date}/{filename}"
        )

    @staticmethod
    def _is_non_fatal_error(msg: str) -> bool:
        msg = msg.lower()
        return any(
            k in msg
            for k in (
                "trading",
                "no data",
                "no such file",
                "not found",
                "550",
                "permission",
                "Failed to change directory"
            )
        )

    # ======================================================
    @logs.catch()
    def _download_one(
            self,
            *,
            ftp: ftplib.FTP,
            date: str,
            plan: dict,
            local_path: Path,
    ) -> None:
        if self.backend == DownloadBackend.CURL:
            self._download_by_curl(date, plan["filename"], local_path)
        else:
            self._download_by_ftplib(ftp, plan["filename"], local_path)

    # ======================================================
    @Retry.decorator(
        exceptions=(ftplib.error_temp, ftplib.error_perm, OSError, TimeoutError),
        max_attempts=2,
        delay=30,
        backoff=2,
        jitter=True,
    )
    def _download_by_ftplib(
            self,
            ftp: ftplib.FTP,
            remote_file: str,
            local_path: Path,
    ) -> None:
        logs.info(f"[FTP] ftplib downloading {remote_file}")
        FileSystem.ensure_dir(local_path.parent)

        with open(local_path, "wb") as fh:
            ftp.retrbinary(f"RETR {remote_file}", fh.write)

    # ======================================================
    @Retry.decorator(
        exceptions=(RuntimeError, OSError),
        max_attempts=3,
        delay=30,
        backoff=2,
        jitter=True,
    )
    def _download_by_curl(
            self,
            date: str,
            remote_file: str,
            local_path: Path,
    ) -> None:
        logs.info(f"[FTP] curl downloading {remote_file}")
        FileSystem.ensure_dir(local_path.parent)

        url = (
            f"ftp://{self.host}:{self.port}"
            f"/{self.remote_root}/{date}/{remote_file}"
        )

        cmd = [
            "curl",
            "--noproxy", "*",
            "--ftp-pasv",
            "-u", f"{self.user}:{self.password}",
            "--fail",
            "--connect-timeout", "30",
            "--retry", "5",
            "--retry-delay", "10",
            "--silent",  # 不刷屏
            "--show-error",
            "--stderr", "-",  # ★ 强制 stderr 输出
            "--write-out",
            (
                "{"
                "\"speed_bps\": %{speed_download}, "
                "\"size_bytes\": %{size_download}"
                "}\n"
            ),  # curl 的下载进度 / 速度信息
            "-o", str(local_path),
            url,
        ]

        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600,
        )
        # if result.stdout.strip():
        #     logs.info(
        #         f"[DownloadStats] {remote_file} | {result.stdout.strip()}"
        #     )

        if result.stdout.strip():
            stats = json.loads(result.stdout)
            speed_mbps = stats["speed_bps"] * 8 / 1_000_000
            size_gb = stats["size_bytes"] / 1_000_000_000

            logs.info(
                f"[DownloadStats] {remote_file} | "
                f"speed={speed_mbps:.2f} Mbps "
                f"size={size_gb:.2f} GB"
            )

        logs.info(
            f"[Download] finished {remote_file} → {local_path}"
        )
