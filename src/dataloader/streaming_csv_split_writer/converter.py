#!filepath: src/dataloader/streaming_csv_split_writer/converter.py

from __future__ import annotations
from pathlib import Path

from src import logs

from .extractor import CsvExtractor
from .parser import CsvBatchParser
from .filters import TickTypeSplitter
from .writers import ParquetFileWriter, SplitWriter
from .router import FileTypeRouter


class StreamingCsvSplitConverter:
    """
    完全 streaming + 高内聚低耦合版本：
    只做“根据 file_type 执行写出逻辑”。
    不做：
        - skip
        - file_type 推断
        - SH/SZ 判断
    这些业务逻辑应该在 Pipeline。
    """

    def __init__(self):
        self.extractor = CsvExtractor()
        self.parser = CsvBatchParser()
        self.splitter = TickTypeSplitter()
        self.router = FileTypeRouter()

    # ===================================================================
    @logs.catch()
    def convert(self, zfile: Path, out_dir: Path, file_type: str):
        logs.info(f"[CSVConvert] 开始处理 {zfile.name} | type={file_type}")

        # Routing（由 file_type 决定写法）
        routes = self.router.route(file_type, out_dir)

        if routes.split:
            # SH_MIXED 情况（Order + Trade）
            writer = SplitWriter(
                out_dir / "SH_Order.parquet",
                out_dir / "SH_Trade.parquet",
            )
        else:
            # 单文件情况
            writer = ParquetFileWriter(Path(routes.single_output))

        # 解压 → 解析 → 流式处理
        byte_stream = self.extractor.extract(zfile)
        reader = self.parser.open_reader(byte_stream)

        total = {"order": 0, "trade": 0, "single": 0}

        for batch in reader:
            batch = self.parser.cast_to_string_batch(batch)

            if routes.split:
                # SH 混合拆分
                order_batch, trade_batch = self.splitter.split(batch)

                if order_batch.num_rows > 0:
                    writer.write_order(order_batch)
                    total["order"] += order_batch.num_rows

                if trade_batch.num_rows > 0:
                    writer.write_trade(trade_batch)
                    total["trade"] += trade_batch.num_rows
            else:
                writer.write(batch)
                total["single"] += batch.num_rows

        writer.close()
        logs.info(f"[CSVConvert] 输出统计: {total}")

