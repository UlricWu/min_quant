#!filepath: src/dataloader/streaming_csv_split_writer/parser.py

from __future__ import annotations
import pyarrow as pa
import pyarrow.csv as csv


class CsvBatchParser:
    """
    将 CSV 字节流解析成 Arrow RecordBatch（流式）
    """

    def open_reader(self, byte_stream):
        return csv.open_csv(
            byte_stream,
            read_options=csv.ReadOptions(
                autogenerate_column_names=False,
                use_threads=True,
                #block_size=1 << 22, # 4mb
                # block_size=1 << 23  # 8MB
                block_size = 1 << 24  # 16MB # todo 根据文件大小动态调节 or 根据可用内存动态调节
            ),
            convert_options=csv.ConvertOptions(
                column_types={},
                null_values=[],
                strings_can_be_null=True,
            ),
        )

    def cast_to_string_batch(self, batch):
        return pa.RecordBatch.from_arrays(
            [col.cast(pa.string()) for col in batch.columns],
            batch.schema.names
        )
