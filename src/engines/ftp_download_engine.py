from __future__ import annotations
from typing import List


class FtpDownloadEngine:
    """
    Engine（冻结契约版）：

    - 只做下载“决策”
    - 不使用当前时间
    - 不做 IO
    """

    @staticmethod
    def resolve_date(date: str) -> str:
        """
        Input:
            date: YYYY-MM-DD

        Output:
            resolved_date: YYYY-MM-DD

        Behavior:
            - date 必须显式给定
            - 非法格式直接抛错
        """
        if not isinstance(date, str):
            raise TypeError("date must be str YYYY-MM-DD")

        parts = date.split("-")
        if len(parts) != 3:
            raise ValueError(f"invalid date format: {date}")

        y, m, d = parts
        if not (len(y) == 4 and len(m) == 2 and len(d) == 2):
            raise ValueError(f"invalid date format: {date}")

        return date

    @staticmethod
    def select_filenames(remote_filenames: List[str]) -> List[str]:
        """
        Input:
            remote_filenames: raw filename list from FTP

        Output:
            selected_filenames: files that should be downloaded

        Rules (冻结):
            - 排除 Bond
            - 排除空字符串
            - 输出顺序确定
        """
        if not isinstance(remote_filenames, list):
            raise TypeError("remote_filenames must be List[str]")

        selected = [
            fn for fn in remote_filenames
            if fn and "Bond" not in fn
        ]

        # 冻结：顺序确定
        return sorted(selected)
