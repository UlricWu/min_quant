# src/utils/parquet_utils.py
from pathlib import Path
import os
import pyarrow.parquet as pq
import pyarrow as pa

from src.utils.filesystem import FileSystem


class ParquetAtomicWriter:
    """
    Parquet 原子写工具（冻结版）

    语义：
      - 永远写到 *.tmp
      - 成功后 rename → 正式 parquet
    """

    @staticmethod
    def write_table(table: pa.Table, output_path: Path, **kwargs) -> None:
        output_path = Path(output_path)
        FileSystem.ensure_dir(output_path.parent)

        tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")

        # 1. 写入临时文件
        pq.write_table(table, tmp_path, **kwargs)

        # 2. fsync（可选但强烈建议）
        with open(tmp_path, "rb") as f:
            os.fsync(f.fileno())

        # 3. 原子替换
        tmp_path.replace(output_path)
