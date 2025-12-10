#!filepath: src/dataloader/sh_converter_streaming.py

from pathlib import Path
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc
from src import logs


class ShConverter:
    ORDER_TYPES = ["A", "D", "M"]
    TRADE_TYPE = "T"
    SPLIT_COL = "TickType"

    def split(self, parquet_path: Path):
        logs.info(f"[ShConverter] Streaming split: {parquet_path}")

        dataset = ds.dataset(str(parquet_path), format="parquet")
        schema = dataset.schema

        if self.SPLIT_COL not in schema.names:
            logs.warning("[ShConverter] 缺少 TickType 字段 → 跳过")
            return

        ticktype_type = schema.field(self.SPLIT_COL).type

        # ---------------------------------------------------------------------
        # Determine real data type for value_set
        # ---------------------------------------------------------------------
        if pa.types.is_dictionary(ticktype_type):
            logs.debug("[ShConverter] TickType 是 dictionary → 使用其 value_type")

            value_type = ticktype_type.value_type

        elif pa.types.is_string(ticktype_type):
            value_type = pa.string()

        elif pa.types.is_binary(ticktype_type):
            value_type = pa.binary()

        else:
            logs.error(f"[ShConverter] 不支持 TickType 类型: {ticktype_type}")
            return

        # ---------------------------------------------------------------------
        # Build Arrow value_set matching TickType value_type
        # ---------------------------------------------------------------------
        if pa.types.is_string(value_type):
            arrow_order_values = pa.array(self.ORDER_TYPES, type=value_type)
            arrow_trade_value = pa.scalar(self.TRADE_TYPE, type=value_type)
        else:
            arrow_order_values = pa.array([v.encode() for v in self.ORDER_TYPES], type=value_type)
            arrow_trade_value = pa.scalar(self.TRADE_TYPE.encode(), type=value_type)

        tick_col = ds.field(self.SPLIT_COL)

        # ---------------------------------------------------------------------
        # Filters (Dataset will auto-decode dictionary)
        # ---------------------------------------------------------------------
        order_filter = pc.is_in(tick_col, value_set=arrow_order_values)
        trade_filter = pc.equal(tick_col, arrow_trade_value)

        # ---------------------------------------------------------------------
        # Execute streaming
        # ---------------------------------------------------------------------
        order_table = dataset.to_table(filter=order_filter)
        trade_table = dataset.to_table(filter=trade_filter)

        out_order = parquet_path.with_name("SH_Order.parquet")
        out_trade = parquet_path.with_name("SH_Trade.parquet")

        pq.write_table(order_table, out_order, compression="zstd")
        pq.write_table(trade_table, out_trade, compression="zstd")

        logs.info(f"[ShConverter] 委托={order_table.num_rows} → {out_order}")
        logs.info(f"[ShConverter] 成交={trade_table.num_rows} → {out_trade}")
        logs.info("[ShConverter] 完成拆分")
