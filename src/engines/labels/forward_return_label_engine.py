#!filepath: src/engines/labels/forward_return_label_engine.py
from __future__ import annotations

from typing import Sequence

import pyarrow as pa
import pyarrow.compute as pc

from src.engines.labels.base import BaseLabelEngine, require_columns


class ForwardReturnLabelEngine(BaseLabelEngine):
    """
    ForwardReturnLabelEngine（FINAL / FROZEN）

    数学定义（row-based）：
        ret[t] = price[t + steps] / price[t] - 1
        or
        log(price[t + steps]) - log(price[t])

    架构约束（不可破）：
        - steps 仅表示“向后多少行”
        - 不涉及分钟 / bar / 时间单位
        - 不跨 symbol（由 LabelBuildStep + SymbolAccessor 保证）
        - 不改变行数
        - 不删除 NaN（尾部 steps 行自然为 null）
    """

    def __init__(
        self,
        *,
        steps: int,
        price_col: str = "close",
        use_log_return: bool = False,
        output_col: str | None = None,
    ) -> None:
        if steps <= 0:
            raise ValueError("steps must be positive")

        self.steps = steps
        self.price_col = price_col
        self.use_log_return = use_log_return

        base = "label_fwd_logret" if use_log_return else "label_fwd_ret"
        self._label_col = output_col or f"{base}_s{steps}"

    # ------------------------------------------------------------------
    # BaseLabelEngine API
    # ------------------------------------------------------------------
    def label_columns(self) -> Sequence[str]:
        return (self._label_col,)

    def execute(self, table: pa.Table) -> pa.Table:
        """
        输入：
            - 单 symbol 子表
            - 行顺序已按时间升序
        输出：
            - append label 列
            - 行数不变
        """
        if table.num_rows == 0:
            return table

        require_columns(
            table,
            ["ts", "symbol", self.price_col],
            who=self.__class__.__name__,
        )

        price = table[self.price_col]

        # row-based shift：future = t + steps
        # future_price = pc.shift(price, -self.steps)
        future_price = _shift_forward(price, self.steps)

        if self.use_log_return:
            label = pc.subtract(pc.ln(future_price), pc.ln(price))
        else:
            label = pc.subtract(pc.divide(future_price, price), 1.0)

        return table.append_column(self._label_col, label)

def _shift_forward(arr: pa.Array, steps: int) -> pa.Array:
    """
    Forward shift (row-based):
      out[i] = arr[i + steps]
      tail filled with null
    """
    if steps <= 0:
        raise ValueError("steps must be positive")

        # --------------------------------------------------
        # Normalize to Array (CRITICAL)
        # --------------------------------------------------
    if isinstance(arr, pa.ChunkedArray):
        arr = arr.combine_chunks()

    n = len(arr)

    if steps >= n:
        return pa.nulls(n, type=arr.type)

    sliced = arr.slice(steps)
    nulls = pa.nulls(steps, type=arr.type)

    return pa.concat_arrays([sliced, nulls])