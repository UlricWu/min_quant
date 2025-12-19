from __future__ import annotations

import ftplib
import subprocess
from enum import Enum
from pathlib import Path
from typing import Optional

from src.adapters.base_adapter import BaseAdapter
from src.engines.ftp_download_engine import FtpDownloadEngine
from src.utils.filesystem import FileSystem
from src.utils.retry import Retry
from src import logs

from src.config.secret_config import SecretConfig
from src.config.pipeline_config import DownloadBackend


class FtpDownloadAdapter(BaseAdapter):
    """
    Adapter 层：
    - 负责 FTP I/O
    - 负责下载实现（ftplib / curl）
    """

    def __init__(
            self,
            secret: SecretConfig,
            backend: DownloadBackend,
            engine: FtpDownloadEngine,
            inst=None,
            remote_root=''
    ):
        super().__init__(inst)
        self._secret = secret
        self.backend = backend
        self.engine = engine
        self.host = self._secret.ftp_host
        self.user = self._secret.ftp_user
        self.password = self._secret.ftp_password
        self.port = self._secret.ftp_port
        self.remote_root = remote_root

    # --------------------------------------------------
    def download_date(self, date: str | None, local_dir: Path):
        date_str = self.engine.resolve_date(date)
        logs.info(f"[FTP] 下载日期: {date_str}")
        FileSystem.ensure_dir(local_dir)

        # --- FTP 只用于列目录 ---
        with self.timer("ftp_connect"):
            ftp = ftplib.FTP(timeout=60)
            ftp.connect(self.host, self.port)
            ftp.login(self.user, self.password)
            # ftp.sock.settimeout(60)

        with self.timer("ftp_cwd"):
            ftp.cwd(self.remote_root)
            ftp.cwd(date_str)

        filenames = ftp.nlst()
        filtered = self.engine.filter_filenames(filenames)

        for fn in filtered:
            local_path = local_dir / fn
            self._download_file(ftp, date_str, fn, local_path)

        ftp.quit()

    # --------------------------------------------------
    def _download_file(
            self,
            ftp: ftplib.FTP,
            date: str,
            remote_file: str,
            local_path: Path,
    ):
        if self.backend == DownloadBackend.CURL:
            self._download_file_by_curl(date, remote_file, local_path)
        else:
            self._download_file_by_ftplib(ftp, remote_file, local_path)

    # --------------------------------------------------
    @Retry.decorator(
        exceptions=(ftplib.error_temp, ftplib.error_perm, OSError, TimeoutError),
        max_attempts=3,
        delay=1,
        backoff=2,
        jitter=True,
    )
    def _download_file_by_ftplib(
            self,
            ftp: ftplib.FTP,
            remote_file: str,
            local_path: Path,
    ):
        logs.info(f"[FTP] ftplib 下载 {remote_file}")
        FileSystem.ensure_dir(local_path.parent)

        with self.timer("ftp_retr"):
            with open(local_path, "wb") as fh:
                ftp.retrbinary(f"RETR {remote_file}", fh.write)

    # --------------------------------------------------
    @Retry.decorator(
        exceptions=(RuntimeError, OSError),
        max_attempts=3,
        delay=1,
        backoff=2,
        jitter=True,
    )
    def _download_file_by_curl(
            self,
            date: str,
            remote_file: str,
            local_path: Path,
    ):
        logs.info(f"[FTP] curl 下载 {remote_file}")
        FileSystem.ensure_dir(local_path.parent)

        url = (
            f"ftp://{self.host}:{self.port}/"
            f"{self.remote_root}/{date}/{remote_file}"
        )

        cmd = [
            "curl",
            "--noproxy", "*",
            "--ftp-pasv",
            "-u", f"{self.user}:{self.password}",
            "--fail",
            "--connect-timeout", "30",
            "--speed-time", "60",
            "--speed-limit", "1024",
            "--retry", "5",
            "--retry-delay", "10",
            "--retry-all-errors",
            "-o", str(local_path),
            url,
        ]

        with self.timer("ftp_retr"):
            result = subprocess.run(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=600,
            )

        if result.returncode != 0:
            raise RuntimeError(
                f"[FTP] curl 失败 {remote_file}: {result.stderr.strip()}"
            )
