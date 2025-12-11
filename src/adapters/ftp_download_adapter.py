from __future__ import annotations
import ftplib
from pathlib import Path
from typing import Optional, List

from src.adapters.base_adapter import BaseAdapter
from src.engines.ftp_download_engine import FtpDownloadEngine
from src.utils.filesystem import FileSystem
from src.utils.retry import Retry
from src import logs


class FtpDownloadAdapter(BaseAdapter):
    """
    Adapter 层：
    - 负责 FTP 连接 I/O
    - 负责文件下载
    - 负责本地写入
    - 可以使用 inst.timer()
    """

    def __init__(
            self,
            host: str,
            user: str,
            password: str,
            port: int,
            engine: FtpDownloadEngine,
            remote_root: str = "",
            inst=None
    ):
        super().__init__(inst)
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.remote_root = remote_root
        self.engine = engine

    # ----------------------------------------------------------------------
    def download_date(self, date: str | None, local_dir: Path):
        """
        下载某一天的数据到 local_dir
        """
        date_str = self.engine.resolve_date(date)
        logs.info(f"[FTP] 下载日期: {date_str}")
        FileSystem.ensure_dir(local_dir)

        with self.timer("ftp_connect"):
            ftp = ftplib.FTP()
            ftp.connect(self.host, self.port)
            ftp.login(self.user, self.password)

        # 进入根目录
        with self.timer("ftp_cwd"):
            ftp.cwd(self.remote_root)

        # 进入日期目录
        try:
            ftp.cwd(date_str)
        except ftplib.error_perm as e:
            logs.error(f"[FTP] 无法进入目录 {date_str}: {e}")
            ftp.quit()
            return

        # 获取所有文件
        filenames = ftp.nlst()
        filtered_files = self.engine.filter_filenames(filenames)

        for fn in filtered_files:
            local_path = local_dir / fn
            self._download_file(ftp, fn, local_path)

        ftp.quit()

    # ----------------------------------------------------------------------
    @Retry.decorator(exceptions=(ftplib.error_temp, ftplib.error_perm, OSError),
                     max_attempts=3, delay=1, backoff=2, jitter=True)
    def _download_file(self, ftp: ftplib.FTP, remote_file: str, local_path: Path):
        logs.info(f"[FTP] 下载文件 {remote_file} → {local_path}")

        with self.timer("ftp_retr"):
            buffer = bytearray()
            ftp.retrbinary(f"RETR {remote_file}", buffer.extend)

        with self.timer("fs_write"):
            FileSystem.safe_write(local_path, bytes(buffer))
