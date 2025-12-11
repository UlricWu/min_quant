from __future__ import annotations
from pathlib import Path
from src.utils.filesystem import FileSystem
from src.adapters.base_adapter import BaseAdapter
from src.engines.csv_convert_engine import CsvConvertEngine
from src.engines.extractor_engine import CsvExtractor
from src.engines.parser import CsvBatchParser
from src.engines.writers import SplitWriter, ParquetFileWriter
from src.engines.router import FileTypeRouter
from src import logs


class CsvConvertAdapter(BaseAdapter):
    """
    Adapter 层：
    - 负责 I/O（解压 / 解析 CSV / 写 parquet）
    - 调用 Engine 做类型统一与拆分逻辑
    - 不做 skip 决策（是否重跑由 Step 决定）
    """

    def __init__(self, engine: CsvConvertEngine, inst=None):
        super().__init__(inst)
        self.engine = engine

        self.extractor = CsvExtractor()
        self.parser = CsvBatchParser()
        self.router = FileTypeRouter()

    # ----------------------------------------------------------------------
    def convert(self, zfile: Path, out_dir: Path, file_type: str):
        logs.info(f"[CSVConvert] 开始处理 {zfile.name} (file_type={file_type})")
        FileSystem.ensure_dir(out_dir)

        # 1) 选择 writer
        writer = self._build_writer(zfile, out_dir, file_type)

        # 2) 解压 CSV
        byte_stream = self.extractor.extract(zfile)

        # 3) CSV → Arrow Reader
        reader = self.parser.open_reader(byte_stream)
        #
        # # 4) 流处理
        with self.timer(f"write_{zfile.name}"):
            for batch in reader:

                # 4.1）统一字段类型
                batch = self.engine.cast_to_string_batch(batch)
                #
                #     # 4.2）拆分或直接写
                # if self.engine.should_split(file_type):
                # 4.2 根据 file_type 决定写法
                if file_type.upper() == "SH_MIXED":
                    order_batch, trade_batch = self.engine.split_sh_mixed(batch)

                    if order_batch.num_rows:
                        writer.write_order(order_batch)

                    if trade_batch.num_rows:
                        writer.write_trade(trade_batch)

                else:
                    writer.write(batch)

            writer.close()
        logs.info(f"[CSVConvert] 完成处理 {zfile.name}")

    # ------------------------------------------------------------------
    def _build_writer(self, zfile: Path, out_dir: Path, file_type: str):
        """
        根据 file_type 构造对应的 Writer：
        - SH_MIXED → SplitWriter(SH_Order.parquet, SH_Trade.parquet)
        - 其他     → ParquetFileWriter(<stem>.parquet)
        """
        if file_type.upper() == "SH_MIXED":
            order_path = out_dir / "SH_Order.parquet"
            trade_path = out_dir / "SH_Trade.parquet"
            return SplitWriter(order_path, trade_path)

        # 普通 SZ / SH 单表
        stem = zfile.stem.replace(".csv", "")
        out_file = out_dir / f"{stem}.parquet"
        return ParquetFileWriter(out_file)
