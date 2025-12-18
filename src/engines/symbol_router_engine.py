from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from src.utils.filesystem import FileSystem
from src import logs


class SymbolRouterEngine:
    """
    执行引擎：按 symbol 拆分 parquet 文件。

    职责：
    - 打开 parquet dataset
    - 流式读取 RecordBatch
    - 按 symbol 写出子 parquet

    不关心：
    - symbols 从哪里来
    - 是否 skip
    - 时间语义 / instrumentation
    """

    def route_file(
            self,
            *,
            date: str,
            kind: str,
            parquet_path: Path,
            symbol_dir: Path,
            symbols: set[str],
    ) -> None:

        dataset = ds.dataset(parquet_path)
        writers: dict[str, pq.ParquetWriter] = {}
        logs.info(f'start parquet file: {parquet_path.name}')
        for batch in dataset.to_batches():
            # SecurityID -> string（已经是 6 位）
            sym_arr = batch["SecurityID"].cast(pa.string())

            # 🔥 只取 batch 中真实存在的 symbol
            unique_syms = pc.unique(sym_arr).to_pylist()

            for sym in unique_syms:
                if sym is None:
                    continue

                # 只处理关心的 symbols
                if sym not in symbols:
                    continue

                mask = pc.equal(sym_arr, sym)
                sub = batch.filter(mask)
                if sub.num_rows == 0:
                    continue

                writer = writers.get(sym)
                if writer is None:
                    out = symbol_dir / sym / date
                    FileSystem.ensure_dir(out)

                    if kind.lower().endswith("order"):
                        out_parquet = out / "Order.parquet"
                    elif kind.lower().endswith("trade"):
                        out_parquet = out / "Trade.parquet"
                    else:
                        raise ValueError(f"Unknown kind: {kind}")

                    writer = pq.ParquetWriter(out_parquet, sub.schema)
                    writers[sym] = writer

                writer.write_batch(sub)

        for w in writers.values():
            w.close()
