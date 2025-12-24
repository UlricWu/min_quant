from __future__ import annotations
from datetime import datetime
from typing import List


class FtpDownloadEngine:
    """
    Engine 层（纯逻辑）：
    - 不做任何 I/O
    - 不依赖 ftplib / Path / OS
    - 只负责：日期、文件选择、远端路径规则
    """

    REMOTE_ROOT = "/level2"

    # --------------------------------------------------
    @staticmethod
    def resolve_date(date: str | None) -> str:
        if date is None:
            return datetime.now().strftime("%Y-%m-%d")

        try:
            dt = datetime.strptime(date, "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            raise ValueError(f"日期格式必须为 YYYY-MM-DD（收到: {date}）")

    # --------------------------------------------------
    @staticmethod
    def filter_filenames(files: List[str]) -> List[str]:
        return [
            f for f in files
            if f and "Bond" not in f
        ]

    # --------------------------------------------------
    def plan_downloads(
        self,
        *,
        date: str,
        available_files: List[str],
    ) -> List[dict]:
        """
        输入：
            date: YYYY-MM-DD
            available_files: FTP LIST 返回的文件名

        输出：
            下载计划（纯 dict，方便 pytest）
        """
        date = self.resolve_date(date)
        files = self.filter_filenames(available_files)

        plans = []
        for fname in files:
            plans.append(
                {
                    "date": date,
                    "filename": fname,
                    "remote_path": f"{self.REMOTE_ROOT}/{date}/{fname}",
                }
            )

        return plans
