#!filepath: src/adapters/symbol_router_adapter.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa

from src.utils.filesystem import FileSystem
from src import logs


class SymbolRouterAdapter:
    """
    Adapter 用于将 Engine 输出的 {symbol: batch}
    写成:
        data/symbol/<symbol>/<date>/<Kind>.parquet

    同时也负责 parquet 输入（dataset.to_batches）
    """

    def __init__(self, path_manager):
        self.pm = path_manager

    # ------------------------------------------------------------------
    def load_parquet_batches(self, parquet_path: Path):
        """
        读取 parquet 为 Arrow batch 迭代器。
        """
        dataset = ds.dataset(parquet_path, format="parquet")
        for batch in dataset.to_batches():
            yield batch

    # ------------------------------------------------------------------
    def write_symbol_batches(
        self,
        date: str,
        kind: str,
        symbol_to_batch: Dict[int, pa.RecordBatch],
    ):
        """
        将 Engine 拆出的 {symbol: sub_batch} 写入磁盘。
        每个 symbol 一个 ParquetWriter（追加模式）。
        """
        for symbol, sub_batch in symbol_to_batch.items():

            # 目标目录 data/symbol/<symbol>/<date>
            out_dir = self.pm.symbol_dir(symbol, date)
            FileSystem.ensure_dir(out_dir)

            out_path = out_dir / f"{kind}.parquet"

            # 增量写入 parquet
            writer = pq.ParquetWriter(out_path, sub_batch.schema)
            writer.write_batch(sub_batch)
            writer.close()

            logs.debug(
                f"[SymbolRouterAdapter] 写入 symbol={symbol}, rows={sub_batch.num_rows}, {out_path}"
            )
