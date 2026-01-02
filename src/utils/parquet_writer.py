#!filepath: src/utils/parquet_writer.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.logger import logs


class ParquetAppendWriter:
    """
    ParquetAppendWriter（最终冻结版）

    语义：
      - 顺序写入一个或多个 Arrow Table
      - 默认 append 语义（batch → batch）
      - 当单个 table 非常大时，自动或显式进行 chunked write
      - 生成单一、schema 稳定、原子落盘的 parquet 文件

    设计裁决（冻结）：
      - Writer 不理解业务
      - Writer 不 sort / concat / index
      - chunk 仅是 I/O 优化，不改变语义
      - schema 在首个写入时冻结
      - 永远 tmp → rename，避免半文件

    使用方式（唯一合法）：
      writer = ParquetAppendWriter(output_file=...)
      writer.write(table)                      # 普通 batch
      writer.write(big_table, max_rows_per_chunk=...)  # 大表（实验路径）
      path = writer.close()
    """

    # --------------------------------------------------
    def __init__(
            self,
            *,
            output_file: Path,
            schema: Optional[pa.Schema] = None,
    ):
        self._final_path = output_file
        self._tmp_path = self._final_path.with_suffix(".parquet.tmp")

        self._schema: Optional[pa.Schema] = schema
        self._writer: Optional[pq.ParquetWriter] = None
        self._rows: int = 0
        self._closed: bool = False

    # ==================================================
    # public API
    # ==================================================
    def write(
            self,
            table: pa.Table,
            *,
            max_rows_per_chunk: Optional[int] = None,
    ) -> None:
        """
        写入一个 Arrow Table。

        - 普通路径：直接 write_table(table)
        - 大表路径：按 chunk 拆分后顺序写入

        参数：
          max_rows_per_chunk:
            - None     → 不拆分
            - int > 0  → table 太大时按该行数拆分
        """
        if table is None or table.num_rows == 0:
            return

        if self._closed:
            raise RuntimeError("[ParquetAppendWriter] write after close()")

        # --------------------------------------------------
        # lazy init writer
        # --------------------------------------------------
        if self._writer is None:
            self._init_writer(table.schema)

        # --------------------------------------------------
        # schema strict check（冻结）
        # --------------------------------------------------
        if table.schema != self._schema:
            raise ValueError(
                "[ParquetAppendWriter] schema mismatch:\n"
                f"expected={self._schema}\n"
                f"got={table.schema}"
            )

        # --------------------------------------------------
        # fast path：不需要 chunk
        # --------------------------------------------------
        if max_rows_per_chunk is None or table.num_rows <= max_rows_per_chunk:
            self._writer.write_table(table)
            self._rows += table.num_rows
            return

        # --------------------------------------------------
        # chunked write（I/O 优化路径）
        # --------------------------------------------------
        start = 0
        total = table.num_rows

        while start < total:
            length = min(max_rows_per_chunk, total - start)
            chunk = table.slice(start, length)
            self._writer.write_table(chunk)
            self._rows += length
            start += length

    # --------------------------------------------------
    def close(self) -> Path:
        """
        关闭 writer，并以原子方式生成最终 parquet 文件
        """
        if self._closed:
            return self._final_path

        if self._writer is not None:
            self._writer.close()
            self._writer = None

        if self._tmp_path.exists():
            self._tmp_path.replace(self._final_path)

        self._closed = True
        return self._final_path

    # --------------------------------------------------
    @property
    def rows(self) -> int:
        return self._rows

    # ==================================================
    # internal helpers
    # ==================================================
    def _init_writer(self, schema: pa.Schema) -> None:
        """
        初始化 ParquetWriter（只允许一次）
        """
        if self._schema is None:
            self._schema = schema
        else:
            if schema != self._schema:
                raise ValueError(
                    "[ParquetAppendWriter] initial schema mismatch:\n"
                    f"expected={self._schema}\n"
                    f"got={schema}"
                )

        self._writer = pq.ParquetWriter(
            self._tmp_path,
            self._schema,
            use_dictionary=True,
            compression="zstd",
            write_statistics=True,
        )
