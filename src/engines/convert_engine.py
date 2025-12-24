
import pyarrow as pa
import pyarrow.compute as pc
from src.engines.extractor_engine import ExtractorEngine
from pathlib import Path
from src.utils.parquet_writer import ParquetAppendWriter

class ConvertEngine:
    ORDER_TYPES = ["A", "D", "M"]
    TRADE_TYPE = "T"
    TICK_COL = "TickType"

    def __init__(self):
        self.extractor = ExtractorEngine
        self.order_set = pa.array(self.ORDER_TYPES)
        self.trade_value = pa.scalar(self.TRADE_TYPE)

    def convert(self, zfile: Path, out_files: dict[str, Path]) -> None:
        reader = self.extractor.open_reader(zfile)
        writer = ParquetAppendWriter()
        try:
            for batch in reader:
                batch = self.extractor.cast_strings(batch)

                if len(out_files) == 1:
                    # 非拆分
                    key = next(iter(out_files))
                    writer.write_batches(out_files[key], [batch])
                else:
                    # 拆分
                    for key, sub_batch in self._split(batch, out_files).items():
                        if sub_batch.num_rows:
                            writer.write_batches(out_files[key], [sub_batch])
        finally:
            writer.close()

    def _split(self, batch: pa.RecordBatch, out_files: dict[str, Path]) -> dict[str, pa.RecordBatch]:
        """返回 (order_batch, trade_batch)"""
        if self.TICK_COL not in batch.schema.names:
            raise ValueError(f"missing column: {self.TICK_COL}")

        idx = batch.schema.get_field_index(self.TICK_COL)
        tick_arr = batch.column(idx)

        order_mask = pc.is_in(tick_arr, self.order_set)
        trade_mask = pc.equal(tick_arr, self.trade_value)

        result = {}
        for key in out_files:
            if "order" in key:
                result[key] = batch.filter(order_mask)
            elif "trade" in key:
                result[key] = batch.filter(trade_mask)

        return result
