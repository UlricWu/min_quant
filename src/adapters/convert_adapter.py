# src/adapters/convert_adapter.py
from __future__ import annotations
from pathlib import Path

from src.engines.extractor_engine import ExtractorEngine
from src.engines.WriterEngine import WriterEngine
from src import logs
from pathlib import Path

import pyarrow as pa


class BaseCsvConvertAdapter:
    """
    SH / SZ 公用的 CSV → parquet 适配器基类。

    职责：
    - 调用 ExtractEngine 打开 CSV Reader
    - 调用 ParserEngine 做批次 cast string
    - 调用 WriterEngine 把若干 batch 写到 parquet

    不关心：
    - 是否要拆分（留给子类决定）
    - 文件名 / path 规则（由 pipeline 决定）
    """

    def __init__(
            self,
            extractor: ExtractorEngine,
            writer: WriterEngine,
            threshold_rows: int = 500_00000,
    ) -> None:
        self.out_path = None
        self.out_file = None
        self.extractor = extractor
        self.writer = writer
        self.threshold_rows = threshold_rows

    # 子类实现：如何处理单个 batch
    def _handle_batch(self, batch: pa.RecordBatch):
        raise NotImplementedError

    # 子类实现：最终 flush
    def _flush(self):
        raise NotImplementedError

    def convert(self, zfile: Path, out_path) -> None:
        self.out_file = out_path / f'{zfile.name.split()[0]}.parquet'
        self.out_path = out_path

        logs.info(f"[CsvConvert] 开始处理: {zfile.name} out_path={self.out_path}")

        reader = self.extractor.open_reader(zfile, streaming=True)

        for batch in reader:
            batch = self.extractor.cast_strings(batch)
            self._handle_batch(batch)

        self._flush()
        logs.info(f"[CsvConvert] 完成: {zfile.name}")


class ConvertAdapter(BaseCsvConvertAdapter):
    """
    适用于：
    - SH_Order.csv.7z → SH_Order.parquet
    - SH_Trade.csv.7z → SH_Trade.parquet
    - SZ_Order.csv.7z → SZ_Order.parquet
    - SZ_Trade.csv.7z → SZ_Trade.parquet

    不需要 TickType 拆分。
    """

    def __init__(
            self,
            extractor,
            writer,
    ) -> None:
        super().__init__(extractor, writer)
        self._bucket: list[pa.RecordBatch] = []
        self._rows = 0

    def _handle_batch(self, batch: pa.RecordBatch):
        self._bucket.append(batch)
        self._rows += batch.num_rows

        if self._rows >= self.threshold_rows:
            logs.debug(f"[ConvertSplit] flush 中间批次 rows={self._rows}")
            self.writer.write_batches(self._bucket, self.out_file)
            self._bucket.clear()
            self._rows = 0

    def _flush(self):
        if not self._bucket:
            return
        logs.debug(f"[ConvertSplit] final flush rows={self._rows}")
        self.writer.write_batches(self._bucket, self.out_file)
        self._bucket.clear()
        self._rows = 0

        # ✅ 新增
        self.writer.close()


class SplitConvertAdapter(BaseCsvConvertAdapter):
    """
    适用于：
    - SH_Stock_OrderTrade.csv.7z → SH_Order.parquet + SH_Trade.parquet

    复用 BaseCsvConvertAdapter 的流程，只是：
    - _handle_batch 里增加 TickType 拆分
    - 分别维护 order_bucket / trade_bucket
    """

    def __init__(
            self,
            extractor,
            splitter,
            writer,
    ) -> None:
        super().__init__(extractor, writer)
        self.splitter = splitter

        self._order_bucket: list[pa.RecordBatch] = []
        self._trade_bucket: list[pa.RecordBatch] = []
        self._rows_order = 0
        self._rows_trade = 0

    def _handle_batch(self, batch: pa.RecordBatch):
        order_batch, trade_batch = self.splitter.split(batch)

        if order_batch.num_rows > 0:
            self._order_bucket.append(order_batch)
            self._rows_order += order_batch.num_rows

        if trade_batch.num_rows > 0:
            self._trade_bucket.append(trade_batch)
            self._rows_trade += trade_batch.num_rows

        self.out_order_path = self.out_path / 'SH_Order.parquet'
        self.out_trade_path = self.out_path / 'SH_Trade.parquet'

        # flush 条件
        if self._rows_order >= self.threshold_rows:
            logs.debug(f"[SplitConvert] flush order rows={self._rows_order}")
            self.writer.write_batches(self._order_bucket, self.out_order_path)
            self._order_bucket.clear()
            self._rows_order = 0

        if self._rows_trade >= self.threshold_rows:
            logs.debug(f"[SplitConvert] flush trade rows={self._rows_trade}")
            self.writer.write_batches(self._trade_bucket, self.out_trade_path)
            self._trade_bucket.clear()
            self._rows_trade = 0

    def _flush(self):
        if self._order_bucket:
            logs.debug(f"[SplitConvert] final flush order rows={self._rows_order}")
            self.writer.write_batches(self._order_bucket, self.out_order_path)
        if self._trade_bucket:
            logs.debug(f"[SplitConvert] final flush trade rows={self._rows_trade}")
            self.writer.write_batches(self._trade_bucket, self.out_trade_path)

        self._order_bucket.clear()
        self._trade_bucket.clear()
        self._rows_order = 0
        self._rows_trade = 0

        # ✅ 新增
        self.writer.close()
