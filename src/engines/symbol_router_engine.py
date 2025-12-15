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

        for batch in dataset.to_batches():
            sym_arr = batch["SecurityID"].cast(pa.string())
            unique_syms = pc.unique(sym_arr).to_pylist()

            for sid in unique_syms:
                sym = str(sid).zfill(6)

                if sym not in symbols:
                    continue
                mask = pc.equal(sym_arr, sym)
                sub = batch.filter(mask)

                writer = writers.get(sym)
                if writer is None:
                    out = symbol_dir / sym / date
                    FileSystem.ensure_dir(out)
                    out_parquet = out / f"{kind}.parquet"
                    writer = pq.ParquetWriter(out_parquet, sub.schema)
                    writers[sym] = writer

                writer.write_batch(sub)

        for w in writers.values():
            w.close()
