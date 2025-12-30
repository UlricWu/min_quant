from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
from src.engines.extractor_engine import ExtractorEngine
from pathlib import Path
from src.utils.parquet_writer import ParquetAppendWriter
from src.pipeline.context import EngineContext



class ConvertEngine:
    ORDER_TYPES = ["A", "D", "M"]
    TRADE_TYPE = "T"
    TICK_COL = "TickType"

    def __init__(self):
        self.extractor = ExtractorEngine
        self.order_set = pa.array(self.ORDER_TYPES)
        self.trade_value = pa.scalar(self.TRADE_TYPE)

    def convert(self, spec: EngineContext) -> None:
        input_file = spec.input_file
        output_file = spec.output_file
        mode = spec.mode
        key = spec.key

        reader = self.extractor.open_reader(input_file)
        writer = ParquetAppendWriter()

        try:
            for batch in reader:
                batch = self.extractor.cast_strings(batch)

                if mode == "full":
                    writer.write_batches(output_file, [batch])
                else:
                    sub = self._split_batch(batch, key)
                    if sub.num_rows:
                        writer.write_batches(output_file, [sub])
        finally:
            writer.close()

    def _split_batch(self, batch: pa.RecordBatch, key: str) -> pa.RecordBatch:
        idx = batch.schema.get_field_index(self.TICK_COL)
        tick_arr = batch.column(idx)

        if "order" in key:
            mask = pc.is_in(tick_arr, self.order_set)
        else:
            mask = pc.equal(tick_arr, self.trade_value)

        return batch.filter(mask)
