import ftplib
from datetime import datetime
from pathlib import Path
from typing import Optional, List

from src.config.app_config import AppConfig
from src.utils.filesystem import FileSystem
from src.utils.retry import Retry
from src.utils.path import PathManager
from src.utils.datetime_utils import DateTimeUtils
from src import logs

cfg = AppConfig.load()


class FTPDownloader:
    """
    Level2 数据下载器（工程级版本）
    ---------------------------------------------------
    ✓ 多次重试
    ✓ 安全写入（tmp → rename）
    ✓ 自动解析日期
    ✓ 默认下载今日
    ✓ 使用 PathManager 管理路径
    ✓ 使用 FileSystem 处理文件
    ✓ 使用 logs 输出日志
    ---------------------------------------------------
    """

    def __init__(self, local_root: Optional[Path] = None):
        # 从 secrets 读取
        # FTP credentials
        self.host = cfg.secret.ftp_host
        self.user = cfg.secret.ftp_user
        self.password = cfg.secret.ftp_password
        self.port = cfg.secret.ftp_port

        if not all([self.host, self.user, self.password]):
            raise RuntimeError("FTP credentials missing: FTP_HOST / FTP_USER / FTP_PASSWORD")

        # FTP 主目录（配置文件 dataloader.remote_dir，例如 "level2"）
        self.remote_root = cfg.data.remote_dir

        # 本地 raw 目录：.../dataloader/raw/
        # ⭐ 本地根目录：允许外部传入
        self.local_root: Path = local_root or PathManager.raw_dir()
        FileSystem.ensure_dir(self.local_root)

    # ---------------------------------------------------------
    # 日期解析：YYYY-MM-DD → 保证为正确格式
    # ---------------------------------------------------------
    @staticmethod
    def _resolve_date(date: Optional[str]) -> str:
        if date is None:
            return datetime.now().strftime("%Y-%m-%d")

        try:
            d = datetime.strptime(date, "%Y-%m-%d")
            return d.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"日期格式必须为 YYYY-MM-DD，例如: 2025-08-29（收到: {date}）")

    def download(self, date: Optional[str] = None) -> None:
        """
                下载指定日期的 Level2 数据。

                参数:
                    date: str | None
                        - '2025-08-29' 这种格式的日期字符串
                        - None 表示下载“今日”数据

                例子:
                    downloader = FTPDownloader()
                    downloader.download("2025-08-29")  # 下载 2025-08-29 当天目录下所有文件
                    downloader.download()              # 不传，默认下载今天
        """
        date_str = self._resolve_date(date)
        logs.info(f"[FTP] 开始下载 Level2 数据，日期: {date_str}")

        local_dir = self.local_root / date_str
        FileSystem.ensure_dir(local_dir)

        # 如果 dates=None → 自动下载今天
        with ftplib.FTP() as ftp:
            logs.info(f"Connecting FTP: host={self.host} port={self.port}")
            ftp.connect(self.host, self.port)  # 自定义端口
            ftp.login(self.user, self.password)

            # 进入根目录，如 "level2"
            logs.debug(f"[FTP] CWD: {self.remote_root}")
            ftp.cwd(self.remote_root)
            # ftp.cwd("level2")

            try:
                # 进入日期子目录 如 "2025-08-29"
                logs.debug(f"[FTP] CWD: {date_str}")
                ftp.cwd(date_str)

                # 列举该目录下所有文件
                filenames = ftp.nlst()
                logs.info(f"Found {len(filenames)} files: {filenames} in {self.remote_root}/{date_str}")

                for fn in filenames:
                    if not fn:
                        continue
                    if 'Bond' in fn:
                        continue  # 暂时跳过债券
                    # SH_Bond_OrderTrade.csv.7z
                    # SH_Stock_OrderTrade.csv.7z
                    # SZ_Order.csv.7z
                    # SZ_Trade.csv.7z

                    local_file = local_dir / fn
                    self._download_file(ftp, fn, local_file)
            except Exception as e:
                logs.error(e)
                logs.debug(f"[FTP] CWD: {date_str} failed: {e}")
                return

        logs.info(f"[FTP] 下载完成，日期: {date_str} → 本地目录: {local_dir}")

    @Retry.decorator(exceptions=(ftplib.error_temp, ftplib.error_perm, OSError),
                     max_attempts=3, delay=1, backoff=2, jitter=True)
    @logs.catch()
    def _download_file(self, ftp: ftplib.FTP, remote_file: str, local_path: Path):
        logs.info(f"[FTP] 下载文件: {remote_file} → {local_path}")

        buffer = bytearray()
        ftp.retrbinary(f"RETR {remote_file}", buffer.extend)

        # 安全原子写入
        FileSystem.safe_write(local_path, bytes(buffer))
        logs.info(f"[FTP] 下载完成 → {local_path}")
