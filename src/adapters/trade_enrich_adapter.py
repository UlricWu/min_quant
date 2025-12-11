#!filepath: src/adapters/trade_enrich_adapter.py

from __future__ import annotations
from pathlib import Path
from typing import Iterable

import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow as pa

from src.utils.filesystem import FileSystem
from src import logs


class TradeEnrichAdapter:
    """
    IO 适配器：
    - parquet → batches
    - enrich(batch)
    - 写入 enriched parquet
    """

    def __init__(self, path_manager):
        self.pm = path_manager

    # ---------------------------------------------------------
    def load_trade_batches(self, trade_path: Path):
        dataset = ds.dataset(trade_path, format="parquet")
        for batch in dataset.to_batches():
            yield batch

    # ---------------------------------------------------------
    def write_enriched_batches(
        self,
        symbol: int,
        date: str,
        batches: Iterable[pa.RecordBatch],
    ):
        out_dir = self.pm.symbol_dir(symbol, date)
        FileSystem.ensure_dir(out_dir)

        out_path = out_dir / "Trade_Enriched.parquet"

        writer = None

        for batch in batches:
            if batch.num_rows == 0:
                continue

            if writer is None:
                writer = pq.ParquetWriter(out_path, batch.schema)

            writer.write_batch(batch)

        if writer:
            writer.close()
            logs.info(
                f"[TradeEnrichAdapter] 写入完成 symbol={symbol}, file={out_path}"
            )
