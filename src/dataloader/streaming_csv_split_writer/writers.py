#!filepath: src/dataloader/streaming_csv_split_writer/writers.py

from __future__ import annotations
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa


class ParquetFileWriter:
    """
    单文件 Writer（增量 write_batch）
    """

    def __init__(self, out_path: Path):
        self.out_path = out_path
        self.writer = None

    def write(self, batch: pa.RecordBatch):
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.out_path, batch.schema)
        self.writer.write_batch(batch)

    def close(self):
        if self.writer:
            self.writer.close()


class SplitWriter:
    """
    用于 SH_MIXED 情况，写 Order / Trade 两个文件
    """

    def __init__(self, order_path: Path, trade_path: Path):
        self.order_writer = ParquetFileWriter(order_path)
        self.trade_writer = ParquetFileWriter(trade_path)

    def write_order(self, batch: pa.RecordBatch):
        self.order_writer.write(batch)

    def write_trade(self, batch: pa.RecordBatch):
        self.trade_writer.write(batch)

    def close(self):
        self.order_writer.close()
        self.trade_writer.close()
