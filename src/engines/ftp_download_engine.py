from __future__ import annotations
from datetime import datetime
from typing import List


class FtpDownloadEngine:
    """
    Engine 层：
    - 不做任何 I/O
    - 不依赖 ftplib / FileSystem / Path
    - 只负责业务逻辑：日期校验、文件选择、路径规则
    """

    @staticmethod
    def resolve_date(date: str | None) -> str:
        """解析日期为 YYYY-MM-DD"""
        if date is None:
            return datetime.now().strftime("%Y-%m-%d")

        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"日期格式必须为 YYYY-MM-DD（收到: {date}）")

    @staticmethod
    def filter_filenames(files: List[str]) -> List[str]:
        """
        过滤掉不需要的文件，如 Bond 等
        这里保持纯逻辑，不依赖任何 I/O
        """
        return [
            f for f in files
            if f and "Bond" not in f
        ]
