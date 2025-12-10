#!filepath: src/l2/offline/feature_batch_sink.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from src import logs
from src.utils.filesystem import FileSystem


class FeatureBatchSink:
    """
    Streaming 特征写出：
    - 按 batch 写入 parquet
    - 不持久保存所有数据在内存中
    """

    def __init__(self, out_path: Path) -> None:
        self.out_path = out_path
        self._writer: Optional[pq.ParquetWriter] = None
        self._columns: Optional[List[str]] = None

    def _ensure_writer(self, rows: List[Dict]) -> None:
        if self._writer is not None:
            return

        if not rows:
            return

        self._columns = sorted(rows[0].keys())
        data = {col: [r.get(col) for r in rows] for col in self._columns}
        table = pa.table(data)

        FileSystem.ensure_dir(self.out_path.parent)
        if self.out_path.exists():
            logs.info(f"[FeatureBatchSink] 删除已存在文件: {self.out_path}")
            self.out_path.unlink()

        self._writer = pq.ParquetWriter(self.out_path, table.schema)
        self._writer.write_table(table)

    def write_rows(self, rows: List[Dict]) -> None:
        if not rows:
            return

        if self._writer is None:
            # 第一次写入时初始化 schema
            self._ensure_writer(rows)
            return

        assert self._columns is not None
        data = {col: [r.get(col) for r in rows] for col in self._columns}
        table = pa.table(data)
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer is not None:
            logs.info(f"[FeatureBatchSink] 关闭 writer: {self.out_path}")
            self._writer.close()
            self._writer = None
