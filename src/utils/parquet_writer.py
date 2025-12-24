# src/utils/parquet_writer.py
from __future__ import annotations

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetAppendWriter:
    def __init__(self):
        self._schemas: dict[Path, pa.Schema] = {}
        self._writers: dict[Path, pq.ParquetWriter] = {}

    def write_batches(self, path: Path, batches: list[pa.RecordBatch]) -> None:
        if not batches:
            return

        writer = self._writers.get(path)
        if writer is None:
            # schema 来自第一个 batch
            schema = batches[0].schema
            path.parent.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(
                path,
                schema,
                compression="zstd",
            )
            self._writers[path] = writer
            self._schemas[path] = schema

        table = pa.Table.from_batches(batches)
        writer.write_table(table)

    def close(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()
