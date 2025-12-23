# src/engines/trade_enrich_engine.py
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


class TradeEnrichEngine:
    """
    TradeEnrichEngine（冻结版）

    输入（契约）：
      - 单 symbol、已按 (symbol, ts) 排序的 Arrow Table
      - 至少包含列：price, volume
      - table.num_rows 允许为 0

    输出：
      - 行数不变
      - 仅新增列：
          - notional: float64  (price * volume)
          - trade_side: int8   (tick rule: +1 / -1 / 0)
    """

    def __init__(
        self,
        *,
        price_col: str = "price",
        volume_col: str = "volume",
        notional_col: str = "notional",
        side_col: str = "trade_side",
    ) -> None:
        self.price_col = price_col
        self.volume_col = volume_col
        self.notional_col = notional_col
        self.side_col = side_col

    # ==========================================================
    # Public API
    # ==========================================================
    def execute(self, table: pa.Table) -> pa.Table:
        if table.num_rows == 0:
            return table

        self._validate_input(table)

        price = table[self.price_col]
        volume = table[self.volume_col]

        # notional = price * volume
        notional = pc.multiply(
            pc.cast(price, pa.float64()),
            pc.cast(volume, pa.float64()),
        )

        # trade_side via tick rule on price
        trade_side = self._infer_trade_side(price)

        # Append columns (schema-safe)
        out = table
        out = self._append_or_replace(out, self.notional_col, notional)
        out = self._append_or_replace(out, self.side_col, trade_side)
        return out

    # ==========================================================
    # Internal helpers
    # ==========================================================
    def _validate_input(self, table: pa.Table) -> None:
        missing = []
        if self.price_col not in table.column_names:
            missing.append(self.price_col)
        if self.volume_col not in table.column_names:
            missing.append(self.volume_col)

        if missing:
            raise ValueError(f"TradeEnrichEngine missing required columns: {missing}")

    def _infer_trade_side(self, price: pa.ChunkedArray | pa.Array) -> pa.Array:
        """
        Tick rule：
          price[i] > price[i-1] -> +1
          price[i] < price[i-1] -> -1
          else -> 0

        说明：
          - 第 0 行 prev_price 为 null -> diff 为 null -> side=0
        """
        # 确保是 Array（而不是 ChunkedArray）
        if isinstance(price, pa.ChunkedArray):
            price = price.combine_chunks()

        n = len(price)
        if n == 0:
            return pa.array([], type=pa.int8())

        # 构造 prev_price = [null] + price[:-1]
        null_head = pa.array([None], type=price.type)
        prev_tail = price.slice(0, n - 1)
        prev_price = pa.concat_arrays([null_head, prev_tail])

        diff = pc.subtract(
            pc.cast(price, pa.float64()),
            pc.cast(prev_price, pa.float64()),
        )

        buy = pc.greater(diff, 0)
        sell = pc.less(diff, 0)

        side = pc.if_else(
            buy,
            pa.scalar(1, pa.int8()),
            pc.if_else(
                sell,
                pa.scalar(-1, pa.int8()),
                pa.scalar(0, pa.int8()),
            ),
        )

        # 确保最终是 int8 array（if_else 通常已推断，但这里更稳）
        return pc.cast(side, pa.int8())

    @staticmethod
    def _append_or_replace(table: pa.Table, name: str, arr: pa.Array) -> pa.Table:
        """
        如果列已存在则替换，否则追加。
        这样可以让 rerun 幂等（不会报重复列名）。
        """
        if name in table.column_names:
            idx = table.column_names.index(name)
            return table.set_column(idx, name, arr)
        return table.append_column(name, arr)
