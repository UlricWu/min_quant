from __future__ import annotations

import ftplib
import subprocess
from pathlib import Path
from typing import List

from src.adapters.base_adapter import BaseAdapter
from src.engines.ftp_download_engine import FtpDownloadEngine
from src.utils.filesystem import FileSystem
from src.utils.retry import Retry
from src import logs

from src.config.secret_config import SecretConfig
from src.config.pipeline_config import DownloadBackend


class FtpDownloadAdapter(BaseAdapter):
    """
    Adapter（冻结版）：

    - 负责 FTP / curl IO
    - 不包含业务决策
    """

    def __init__(
        self,
        secret: SecretConfig,
        backend: DownloadBackend,
        engine: FtpDownloadEngine,
        inst=None,
        remote_root: str = "",
    ):
        super().__init__(inst)
        self.secret = secret
        self.backend = backend
        self.engine = engine
        self.remote_root = remote_root

    # --------------------------------------------------

    def list_remote_files(self, date: str) -> List[str]:
        logs.info(f"[FTP] list remote files: {date}")

        ftp = ftplib.FTP(timeout=60)
        ftp.connect(self.secret.ftp_host, self.secret.ftp_port)
        ftp.login(self.secret.ftp_user, self.secret.ftp_password)

        ftp.cwd(self.remote_root)
        ftp.cwd(date)

        files = ftp.nlst()
        ftp.quit()
        return files

    # --------------------------------------------------

    def download_files(
        self,
        date: str,
        filenames: List[str],
        local_dir: Path,
    ) -> None:
        FileSystem.ensure_dir(local_dir)

        ftp = ftplib.FTP(timeout=60)
        ftp.connect(self.secret.ftp_host, self.secret.ftp_port)
        ftp.login(self.secret.ftp_user, self.secret.ftp_password)

        ftp.cwd(self.remote_root)
        ftp.cwd(date)

        for fn in filenames:
            local_path = local_dir / fn
            self._download_one(ftp, date, fn, local_path)

        ftp.quit()

    # --------------------------------------------------

    def _download_one(
        self,
        ftp: ftplib.FTP,
        date: str,
        filename: str,
        local_path: Path,
    ):
        if self.backend == DownloadBackend.CURL:
            self._download_by_curl(date, filename, local_path)
        else:
            self._download_by_ftplib(ftp, filename, local_path)

    # --------------------------------------------------

    @Retry.decorator(
        exceptions=(ftplib.error_temp, ftplib.error_perm, OSError),
        max_attempts=3,
        delay=1,
        backoff=2,
    )
    def _download_by_ftplib(
        self,
        ftp: ftplib.FTP,
        filename: str,
        local_path: Path,
    ):
        logs.info(f"[FTP] download {filename}")
        FileSystem.ensure_dir(local_path.parent)

        with open(local_path, "wb") as fh:
            ftp.retrbinary(f"RETR {filename}", fh.write)

    # --------------------------------------------------

    @Retry.decorator(
        exceptions=(RuntimeError, OSError),
        max_attempts=3,
        delay=1,
        backoff=2,
    )
    def _download_by_curl(
        self,
        date: str,
        filename: str,
        local_path: Path,
    ):
        logs.info(f"[FTP] curl download {filename}")
        FileSystem.ensure_dir(local_path.parent)

        url = (
            f"ftp://{self.secret.ftp_host}:{self.secret.ftp_port}/"
            f"{self.remote_root}/{date}/{filename}"
        )

        cmd = [
            "curl",
            "--noproxy", "*",      # 🔥 关键
            "--ftp-pasv",
            "-u", f"{self.secret.ftp_user}:{self.secret.ftp_password}",
            "--fail",
            "-o", str(local_path),
            url,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
