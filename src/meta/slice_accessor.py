# src/meta/slice_accessor.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from src.meta.slice_capability import SliceCapability


class SliceAccessor:
    """
    SliceAccessor（FINAL · READ-ONLY INDEX ENGINE）

    职责（冻结）：
      - 只读 parquet
      - 只根据 index slice
      - 不写文件
      - 不做业务
      - 不理解 pipeline
    """

    # --------------------------------------------------
    def __init__(
        self,
        *,
        parquet_file: Path,
        capability: SliceCapability,
    ) -> None:
        self._parquet_file = parquet_file
        self._cap = capability

        self._table: pa.Table | None = None

    # --------------------------------------------------
    @classmethod
    def from_manifest(
        cls,
        *,
        parquet_file: Path,
        index: Dict[str, Tuple[int, int]],
    ) -> "SliceAccessor":
        cap = SliceCapability(
            type="symbol",
            index=index,
        )
        return cls(
            parquet_file=parquet_file,
            capability=cap,
        )

    # --------------------------------------------------
    def _load_table(self) -> pa.Table:
        if self._table is None:
            self._table = pq.read_table(self._parquet_file)
        return self._table

    # --------------------------------------------------
    def keys(self) -> list[str]:
        return self._cap.keys()

    # --------------------------------------------------
    def get(self, key: str) -> pa.Table:
        start, length = self._cap.bounds(key)
        table = self._load_table()
        return table.slice(start, length)
