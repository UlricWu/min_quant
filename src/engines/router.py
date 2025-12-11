#!filepath: src/dataloader/streaming_csv_split_writer/router.py

from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass


@dataclass
class FileTypeRoutes:
    """
    file_type → 输出文件名映射
    """
    single_output: str | None = None
    split: bool = False


class FileTypeRouter:
    """
    将 file_type 转换为写出策略
    """

    def route(self, file_type: str, out_dir: Path):
        if file_type == "SH_MIXED":
            return FileTypeRoutes(
                split=True,
                single_output=None,
            )

        mapping = {
            "SH_ORDER": "SH_Order.parquet",
            "SH_TRADE": "SH_Trade.parquet",
            "SZ_ORDER": "SZ_Order.parquet",
            "SZ_TRADE": "SZ_Trade.parquet",
        }

        if file_type not in mapping:
            raise RuntimeError(f"未知 file_type: {file_type}")

        return FileTypeRoutes(
            split=False,
            single_output=str(out_dir / mapping[file_type])
        )
