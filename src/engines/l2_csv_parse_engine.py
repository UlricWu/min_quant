# src/l2/engines/l2_csv_parse_engine.py

from __future__ import annotations
import pyarrow as pa
import pyarrow.compute as pc

class L2CsvParseEngine:
    """
    纯逻辑层：
    - 批处理 Arrow RecordBatch（不负责 I/O）
    - 强制字段转 string
    - 清理脏数据
    - 只返回新的 batch，不写 parquet
    """

    def cast_all_to_string(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        cols = {}
        for name, col in zip(batch.schema.names, batch.columns):
            cols[name] = col.cast(pa.string())
        return pa.RecordBatch.from_arrays(list(cols.values()), list(cols.keys()))

    def split_by_ticktype(
        self, batch: pa.RecordBatch,
        order_types=("A", "D", "M"),
        trade_type="T",
        tick_col="TickType",
    ):
        tick = batch[tick_col]

        order_mask = pc.is_in(tick, value_set=pa.array(order_types, type=pa.string()))
        trade_mask = pc.equal(tick, pa.scalar(trade_type, type=pa.string()))

        return (
            batch.filter(order_mask),
            batch.filter(trade_mask)
        )
