from __future__ import annotations

from pathlib import Path
from typing import Iterable

from src import PathManager
from src.meta.base import BaseMeta, MetaOutput
from src.utils.filesystem import FileSystem
from src.utils.logger import logs
import pandas as pd


class SourceMetaRepairTool:
    """
    SourceMetaRepairTool（冻结版）

    职责：
      - 为已存在的 raw 文件补写 / 重建 Source 层 meta
      - 只作用于 /mnt/cold/raw/<date>/*

    冻结约束（Hard Freeze）：
      1. 不允许任何网络 / 下载 / FTP 行为
      2. 不读取、推断、修改任何下游数据
      3. input_file == output_file
      4. rows 永远为 0
      5. stage 永远为 "download"
    """

    STAGE = "download"

    def __init__(
            self,
            pm: PathManager
    ) -> None:
        self.pm = pm

    # ------------------------------------------------------------------
    def repair_date(self, date: str) -> None:
        """
        仅修复指定 date 的 raw source meta
        """
        meta_dir = self.pm.meta_dir(date)
        FileSystem.ensure_dir(meta_dir)

        raw_date_dir = self.pm.raw_dir(date)

        raw_files = sorted(
            p for p in raw_date_dir.iterdir()
            if p.is_file()
        )

        if not raw_files:
            logs.warning(f"[SourceMetaRepair] no raw files found: {raw_date_dir}")
            return

        for raw_file in raw_files:
            self._repair_one(raw_file, meta_dir)

    def repair_range(self, start: str, end: str) -> None:
        """
        修复 [start, end] 区间内的 raw source meta（逐日）
        """

        dates = pd.date_range(start=start, end=end, freq="D")

        for current in dates:
            date_str = current.strftime("%Y-%m-%d")
            logs.info(f"[SourceMetaRepair] processing date={date_str}")

            try:
                self.repair_date(date_str)
            except Exception as e:
                # ❗ 单日失败不影响区间整体
                logs.error(
                    f"[SourceMetaRepair] failed | date={date_str} | {e}"
                )

    # ------------------------------------------------------------------
    def _repair_one(self, raw_file: Path, meta_dir: Path) -> None:
        meta = BaseMeta(
            meta_dir=meta_dir,
            stage=self.STAGE,
            output_slot=raw_file.name,
        )

        if meta.exists() and not meta.upstream_changed():
            logs.info(f"[SourceMetaRepair] meta hit → skip {raw_file.name}")
            return

        logs.info(f"[SourceMetaRepair] repairing meta → {raw_file.name}")

        meta.commit(
            MetaOutput(
                input_file=raw_file,
                output_file=raw_file,
                rows=0,
            )
        )
