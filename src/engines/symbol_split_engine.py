#!filepath: src/engines/symbol_split_engine.py
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
from typing import Iterable
from src import logs

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
        self.symbol_field = symbol_field

    # --------------------------------------------------
    def split_one(
            self,
            table: pa.Table,
            symbol: str,
    ) -> bytes:
        """
        从 canonical table 中切出某一个 symbol
        """
        mask = pa.compute.equal(table[self.symbol_field], symbol)
        sub = table.filter(mask)

        sink = pa.BufferOutputStream()
        pq.write_table(sub, sink)

        return sink.getvalue().to_pybytes()

    # --------------------------------------------------
    @logs.catch()
    def split_many(
            self,
            table: pa.Table,
            symbols: Iterable[str],
    ) -> dict[str, bytes]:
        """
        一次切多个 symbol（可选优化）
        """
        result: dict[str, bytes] = {}

        for sym in symbols:
            result[sym] = self.split_one(table, sym)

        return result
