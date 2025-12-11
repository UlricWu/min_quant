#!filepath: src/engines/csv_convert_engine.py
from __future__ import annotations

from typing import Tuple

import pyarrow as pa
import pyarrow.compute as pc


class CsvConvertEngine:
    """
    负责 CSV → Arrow Batch 的纯“业务逻辑”部分：

    - 统一列类型（全部 cast 到 string）
    - 对 SH_MIXED 文件按 TickType 拆分为 Order / Trade
    - 不做任何 I/O（不读写文件，不解压，不操作路径）
    """

    ORDER_TICK_TYPES = ("A", "D", "M")
    TRADE_TICK_TYPE = "T"
    TICK_COL = "TickType"

    # ------------------------------------------------------------------
    def cast_to_string_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        将 batch 中所有列统一 cast 为 string，避免后续类型不一致问题。
        """
        names = batch.schema.names
        new_arrays = []

        for i, name in enumerate(names):
            col = batch.column(i)
            if not pa.types.is_string(col.type):
                col = col.cast(pa.string())
            new_arrays.append(col)

        return pa.RecordBatch.from_arrays(new_arrays, names)

    # ------------------------------------------------------------------
    def split_sh_mixed(
        self, batch: pa.RecordBatch
    ) -> Tuple[pa.RecordBatch, pa.RecordBatch]:
        """
        针对 SH_MIXED（SH_Stock_OrderTrade）：
        - 使用 TickType 列拆分出 Order / Trade 两个子 batch
        """
        if self.TICK_COL not in batch.schema.names:
            raise KeyError(f"Batch 缺少列: {self.TICK_COL}")

        idx = batch.schema.get_field_index(self.TICK_COL)
        tick_arr = batch.column(idx)

        # 订单：TickType in [A, D, M]
        order_values = pa.array(self.ORDER_TICK_TYPES, type=pa.string())
        order_mask = pc.is_in(tick_arr, value_set=order_values)

        # 成交：TickType == "T"
        trade_mask = pc.equal(tick_arr, pa.scalar(self.TRADE_TICK_TYPE, type=pa.string()))

        order_batch = batch.filter(order_mask)
        trade_batch = batch.filter(trade_mask)

        return order_batch, trade_batch
