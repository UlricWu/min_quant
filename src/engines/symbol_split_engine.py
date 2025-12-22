#!filepath: src/engines/symbol_split_engine.py
from __future__ import annotations

from collections import defaultdict

import pyarrow as pa
import pyarrow.parquet as pq
from typing import Iterable, Optional

from pandas import options

from src import logs
import pyarrow.compute as pc


class SymbolSplitEngine:
    """
    SymbolSplitEngine（纯逻辑）：

    Input:
        - canonical Events.parquet（Arrow Table / Reader）
        - symbol: str

    Output:
        - bytes（该 symbol 的 parquet 内容）

    约束：
        - 不做 IO
        - 不依赖 Path
        - 不接触 Meta
    """

    def __init__(self, symbol_field: str = "symbol"):
        self.symbol_col = symbol_field

    # --------------------------------------------------
    def split_one(
            self,
            table: pa.Table,
            symbol: str,
    ) -> bytes:
        """
        从 canonical table 中切出某一个 symbol
        """
        mask = pa.compute.equal(table[self.symbol_col], symbol)
        sub = table.filter(mask)

        sink = pa.BufferOutputStream()
        pq.write_table(sub, sink)

        return sink.getvalue().to_pybytes()

    # --------------------------------------------------
    @logs.catch()
    def split_many(
            self,
            table: pa.Table,
            needs_symbol: list = [],
            batch_size: int = 200_000,
    ) -> dict[str, bytes]:
        """
        一次切多个 symbol（可选优化）
        """
        """
                返回：
                    symbol -> Arrow Table
                """
        # ------------------------------------------------------------------
        # 0️⃣ 关键：合并 chunk（必须做）
        # ------------------------------------------------------------------
        table = table.combine_chunks()

        symbols = table[self.symbol_col].to_pylist()
        num_rows = table.num_rows

        # symbol -> row indices
        index_map: dict[str, list[int]] = defaultdict(list)

        # ------------------------------------------------------------------
        # 1️⃣ 一次顺序扫描
        # ------------------------------------------------------------------
        for i in range(num_rows):
            sym = symbols[i]
            if sym not in needs_symbol:
                continue
            index_map[sym].append(i)

        # ------------------------------------------------------------------
        # 2️⃣ 生成子表
        # ------------------------------------------------------------------
        result: dict[str, pa.Table] = {}

        for sym, indices in index_map.items():
            idx_array = pa.array(indices, type=pa.int32())
            result[sym] = table.take(idx_array)

        return result
