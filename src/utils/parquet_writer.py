# src/utils/parquet_writer.py
from __future__ import annotations

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq


class ParquetAppendWriter:
    def __init__(self, path: Path, schema: pa.Schema):
        self.path = path
        self.schema = schema
        self._writer: pq.ParquetWriter | None = None

    def write(self, table: pa.Table) -> None:
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(
                self.path, self.schema, compression="zstd"
            )
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer:
            self._writer.close()
            self._writer = None
