#!filepath: src/engines/writer_engine.py
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

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


class StreamingWriterEngine(WriterEngine):
    """
    高性能 streaming 版：
    - 不做 concat_tables
    - 逐 batch 写入 ParquetWriter（RowGroup）
    - 不持久化状态，由 Adapter 控制何时结束
    """

    @staticmethod
    def write_batches(batches: list[pa.RecordBatch], out_path: Path) -> None:
        logs.info(f'Writing {len(batches)} batches to {out_path}')
        if not batches:
            return

        # 用第一个 batch 定 schema
        first = batches[0]
        writer = pq.ParquetWriter(
            out_path,
            first.schema,
            compression="zstd",
            use_dictionary=True,
            write_statistics=True,
        )

        try:
            for batch in batches:
                writer.write_batch(batch)
        finally:
            writer.close()
