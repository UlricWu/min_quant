#!filepath: src/engines/writer_engine.py
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Dict

import pyarrow as pa
import pyarrow.parquet as pq
from src import logs

class WriterEngine(ABC):
    """
    Writer 抽象基类：
    - 定义统一接口
    - 便于注入不同实现（普通版 / 高性能 streaming 版）
    """

    @staticmethod
    def write_batches(batches: list[pa.RecordBatch], out_path: Path) -> None:
        ...


class SimpleWriterEngine(WriterEngine):
    """
    MVP 普通版：
    - 把所有 batch 一次性 concat 成 Table 再写 parquet
    - 适合小文件 / 单元测试 / 验证逻辑
    """

    @staticmethod
    def write_batches(batches: list[pa.RecordBatch], out_path: Path) -> None:
        logs.info(f'Writing {len(batches)} batches to {out_path}')

        if not batches:
            return
        table = pa.Table.from_batches(batches).combine_chunks()
        pq.write_table(table, out_path, compression="zstd")


# class StreamingWriterEngine(WriterEngine):
#     """
#     高性能 streaming 版：
#     - 不做 concat_tables
#     - 逐 batch 写入 ParquetWriter（RowGroup）
#     - 不持久化状态，由 Adapter 控制何时结束
#     """
#
#     @staticmethod
#     def write_batches(batches: list[pa.RecordBatch], out_path: Path) -> None:
#         logs.info(f'Writing {len(batches)} batches to {out_path}')
#         if not batches:
#             return
#
#         # 用第一个 batch 定 schema
#         first = batches[0]
#         writer = pq.ParquetWriter(
#             out_path,
#             first.schema,
#             compression="zstd",
#             use_dictionary=True,
#             write_statistics=True,
#         )
#
#         try:
#             for batch in batches:
#                 writer.write_batch(batch)
#         finally:
#             writer.close()

class StreamingWriterEngine(WriterEngine):
    """
    高性能 Parquet Writer Engine

    特点：
    - ParquetWriter 长生命周期
    - flush = write_row_group
    - 最终 close 才写 footer
    """

    def __init__(
        self,
        compression: str = "zstd",
        row_group_size: int = 1_000_000,
    ) -> None:
        self.compression = compression
        self.row_group_size = row_group_size

        # 每个 out_file 对应一个 writer
        self._writers: Dict[Path, pq.ParquetWriter] = {}

    def _get_writer(self, out_file: Path, schema: pa.Schema) -> pq.ParquetWriter:
        writer = self._writers.get(out_file)
        if writer is None:
            writer = pq.ParquetWriter(
                out_file,
                schema,
                compression=self.compression,
                use_dictionary=True,
            )
            self._writers[out_file] = writer
        return writer

    def write_batches(
        self,
        batches: List[pa.RecordBatch],
        out_file: Path,
    ) -> None:
        if not batches:
            return

        # ⚠️ 不 concat 成一个大 table，直接写 row group
        table = pa.Table.from_batches(batches)
        writer = self._get_writer(out_file, table.schema)

        writer.write_table(
            table,
            row_group_size=self.row_group_size,
        )

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()