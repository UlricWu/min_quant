#!filepath: src/engines/symbol_index_engine.py
from __future__ import annotations

from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.compute as pc
from src.utils.logger import logs


class SymbolIndexEngine:
    """
    SymbolIndexEngine（FINAL · Aggregate Engine）

    语义：
      - 对全量 Arrow Table 执行：
        1) 全局 sort（symbol asc, ts asc）
        2) 构建 symbol slice index

    输入：
      - pa.Table（全量）

    输出：
      - sorted_table: pa.Table
      - index: Dict[str, Tuple[start, length]]

    冻结约束：
      - 只允许在全量视角调用
      - 不允许在 batch loop 中调用
      - 不涉及 I/O
    """

    # --------------------------------------------------
    @staticmethod
    @logs.catch()
    def execute(
            table: pa.Table,
    ) -> tuple[pa.Table, Dict[str, Tuple[int, int]]]:

        if table is None or table.num_rows == 0:
            return pa.table([]), {}

        # --------------------------------------------------
        # 0) normalize symbol column type (CRITICAL)
        # --------------------------------------------------
        if "symbol" not in table.column_names:
            raise KeyError("[SymbolIndexEngine] missing 'symbol' column")

        sym = table["symbol"]
        if pa.types.is_dictionary(sym.type):
            table = table.set_column(
                table.column_names.index("symbol"),
                "symbol",
                pc.cast(sym, pa.string()),
            )
        elif not pa.types.is_string(sym.type):
            raise TypeError(
                f"[SymbolIndexEngine] invalid symbol type: {sym.type}"
            )

        # --------------------------------------------------
        # 1) global sort（明确且显式）
        # --------------------------------------------------
        sort_indices = pc.sort_indices(
            table,
            sort_keys=[
                ("symbol", "ascending"),
                ("ts", "ascending"),
            ],
        )
        table = table.take(sort_indices)

        # --------------------------------------------------
        # 2) build symbol slice index
        # --------------------------------------------------
        sym = table["symbol"]

        if pa.types.is_dictionary(sym.type):
            sym = pc.cast(sym, pa.string())
        elif not pa.types.is_string(sym.type):
            raise TypeError(
                f"[SymbolIndexEngine] invalid symbol type: {sym.type}"
            )

        ree = pc.run_end_encode(sym).combine_chunks()
        run_ends = ree.run_ends.to_pylist()
        values = ree.values.to_pylist()

        index: Dict[str, Tuple[int, int]] = {}
        start = 0

        for symbol, end_exclusive in zip(values, run_ends):
            end_exclusive = int(end_exclusive)
            index[str(symbol)] = (start, end_exclusive - start)
            start = end_exclusive

        return table, index
