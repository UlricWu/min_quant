# src/adapters/convert_adapter.py
from __future__ import annotations
from pathlib import Path

from src.engines.extractor_engine import ExtractorEngine
from src.engines.writer_engine import WriterEngine
from src import logs
from pathlib import Path

import pyarrow as pa

class BaseCsvConvertAdapter:
    """
    CsvConvert Adapter（契约版）：

    Input:
        zfile: Path
        out_files: Dict[str, Path]   # ← Step 决定

    Effect:
        - 写 parquet
    """

    def __init__(self, extractor: ExtractorEngine, writer: WriterEngine):
        self.extractor = extractor
        self.writer = writer

    def convert(self, zfile: Path, out_files: dict[str, Path]) -> None:
        reader = self.extractor.open_reader(zfile)

        for batch in reader:
            batch = self.extractor.cast_strings(batch)
            self._handle_batch(batch, out_files)

        self._flush(out_files)

    def _handle_batch(self, batch: pa.RecordBatch, out_files: dict[str, Path]):
        raise NotImplementedError

    def _flush(self, out_files: dict[str, Path]):
        raise NotImplementedError

class ConvertAdapter(BaseCsvConvertAdapter):
    """
    非拆分型：
        out_files = {"default": <path>}
    """

    def __init__(self, extractor, writer, threshold_rows=50_000_000):
        super().__init__(extractor, writer)
        self._bucket = []
        self._rows = 0
        self.threshold_rows = threshold_rows

    def _handle_batch(self, batch, out_files):
        self._bucket.append(batch)
        self._rows += batch.num_rows

        if self._rows >= self.threshold_rows:
            self.writer.write_batches(self._bucket, out_files["default"])
            self._bucket.clear()
            self._rows = 0

    def _flush(self, out_files):
        if self._bucket:
            self.writer.write_batches(self._bucket, out_files["default"])
        self._bucket.clear()
        self._rows = 0
        self.writer.close()

class SplitConvertAdapter(BaseCsvConvertAdapter):
    """
    拆分型：
        out_files = {
            "order": Path,
            "trade": Path,
        }
    """

    def __init__(self, extractor, splitter, writer, threshold_rows=50_000_000):
        super().__init__(extractor, writer)
        self.splitter = splitter
        self.threshold_rows = threshold_rows

        self._order_bucket = []
        self._trade_bucket = []
        self._rows_order = 0
        self._rows_trade = 0

    def _handle_batch(self, batch, out_files):
        order_batch, trade_batch = self.splitter.split(batch)

        if order_batch.num_rows:
            self._order_bucket.append(order_batch)
            self._rows_order += order_batch.num_rows

        if trade_batch.num_rows:
            self._trade_bucket.append(trade_batch)
            self._rows_trade += trade_batch.num_rows

        if self._rows_order >= self.threshold_rows:
            self.writer.write_batches(self._order_bucket, out_files["order"])
            self._order_bucket.clear()
            self._rows_order = 0

        if self._rows_trade >= self.threshold_rows:
            self.writer.write_batches(self._trade_bucket, out_files["trade"])
            self._trade_bucket.clear()
            self._rows_trade = 0

    def _flush(self, out_files):
        if self._order_bucket:
            self.writer.write_batches(self._order_bucket, out_files["order"])
        if self._trade_bucket:
            self.writer.write_batches(self._trade_bucket, out_files["trade"])

        self._order_bucket.clear()
        self._trade_bucket.clear()
        self._rows_order = 0
        self._rows_trade = 0
        self.writer.close()
