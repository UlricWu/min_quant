from pathlib import Path
from typing import Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.filesystem import FileSystem
from src import logs



class ShConverter:
    """
    上交所逐笔混合文件拆分器（严谨版）

    SH 逐笔类型:
        A : 新增委托 → Order
        D : 删除委托 → Order
        T : 成交       → Trade
    """

    ORDER_TYPES = ("A", "D", "M")   # 上交所委托事件（包含未来可能出现的 Modify）
    TRADE_TYPE = ("T",)

    SPLIT_TYPE = "TickType"

    def __init__(self):
        pass

    # -----------------------------------------------------
    @logs.catch()
    def split(self, parquet_path: Path) :
        """
        输入 SH 混合 parquet → 输出:
            <stem>_Order.parquet
            <stem>_Trade.parquet
        """

        logs.info(f"[ShConverter] 拆分混合文件: {parquet_path}")

        table = pq.read_table(parquet_path)

        if self.SPLIT_TYPE not in table.schema.names:
            logs.warning(
                f"[ShConverter] 缺少 TickType 字段（非上交所混合文件？）: {parquet_path}"
            )
            return

        tick_col = table.column(self.SPLIT_TYPE).to_pylist()

        # -----------------------------
        # Trade: TickType == 'T'
        # -----------------------------
        trade_mask = pa.array([(t == "T") for t in tick_col])
        trade_table = table.filter(trade_mask)

        # -----------------------------
        # Order: TickType in {'A','D','M'}
        # -----------------------------
        order_mask = pa.array([(t in self.ORDER_TYPES) for t in tick_col])
        order_table = table.filter(order_mask)

        # -----------------------------
        # 输出路径
        # -----------------------------

        out_order = parquet_path.with_name("SH_Order.parquet")
        out_trade = parquet_path.with_name("SH_Trade.parquet")

        pq.write_table(order_table, out_order, compression="zstd")
        pq.write_table(trade_table, out_trade, compression="zstd")

        logs.info(f"[ShConverter] 输出委托表: {out_order}")
        logs.info(f"[ShConverter] 输出成交表: {out_trade}")

        # return out_order, out_trade

    # -----------------------------------------------------
    def is_sh_mixed_file(self, parquet_path: Path) -> bool:
        """
        判断是否为 SH 混合文件：
        - TickType 字段存在
        - 同时包含 Order 类型（A/D/M）和成交类型（T）
        """
        try:
            table = pq.read_table(parquet_path, columns=[self.SPLIT_TYPE])
        except:
            return False

        tick_vals = set(table.column(self.SPLIT_TYPE).to_pylist())
        has_trade = "T" in tick_vals
        has_order = any(t in self.ORDER_TYPES for t in tick_vals)

        return has_trade and has_order
