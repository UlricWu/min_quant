#!filepath: src/dataloader/streaming_csv_split_writer/filters.py

from __future__ import annotations
import pyarrow as pa
import pyarrow.compute as pc


class TickTypeSplitter:
    """
    SH 混合文件拆分逻辑（Order / Trade）
    """

    ORDER_TYPES = ["A", "D", "M"]
    TRADE_TYPE = "T"
    TICK_COL = "TickType"

    def __init__(self):
        self.order_set = pa.array(self.ORDER_TYPES, type=pa.string())
        self.trade_value = pa.scalar(self.TRADE_TYPE, type=pa.string())

    def split(self, batch: pa.RecordBatch):
        """返回 (order_batch, trade_batch)"""
        idx = batch.schema.get_field_index(self.TICK_COL)
        tick_arr = batch.column(idx)

        order_mask = pc.is_in(tick_arr, self.order_set)
        trade_mask = pc.equal(tick_arr, self.trade_value)

        return (
            batch.filter(order_mask),
            batch.filter(trade_mask),
        )
