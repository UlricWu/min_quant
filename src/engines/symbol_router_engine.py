#!filepath: src/engines/symbol_router_engine.py
from __future__ import annotations

from typing import Dict

import pyarrow as pa
import pyarrow.compute as pc


class SymbolRouterEngine:
    """
    SymbolRouterEngine（契约最终版）

    Input:
        - pa.Table | pa.RecordBatch
        - 必须包含列: 'Symbol' (string)

    Output:
        - Dict[symbol, pa.Table]
        - schema 与输入完全一致
        - 行顺序保持（filter 保持相对顺序）

    禁止：
        - IO（读 parquet / 写 parquet）
        - symbol 白名单过滤
        - schema 映射（不处理 SecurityID 等别名）
    """

    SYMBOL_COL = "symbol"  # SecurityID

    def split(self, data: pa.Table | pa.RecordBatch) -> Dict[str, pa.Table]:
        table = self._to_table(data)
        self._validate(table)

        # --------------------------------------------------
        # 1) 只合并 Symbol 列（非常小）
        # --------------------------------------------------
        sym_chunked: pa.ChunkedArray = table[self.SYMBOL_COL]
        sym_array: pa.Array = sym_chunked.combine_chunks()

        # --------------------------------------------------
        # 2) dictionary_encode（返回 DictionaryArray）
        # --------------------------------------------------
        encoded: pa.DictionaryArray = pc.dictionary_encode(sym_array)
        dictionary: pa.Array = encoded.dictionary
        indices: pa.Array = encoded.indices

        # --------------------------------------------------
        # 3) 按 dictionary 分组（O(N)）
        # --------------------------------------------------
        out: Dict[str, pa.Table] = {}

        for idx, sym in enumerate(dictionary.to_pylist()):
            if sym is None:
                continue

            mask = pc.equal(indices, pa.scalar(idx, indices.type))
            sub = table.filter(mask)

            if sub.num_rows > 0:
                out[str(sym)] = sub

        return out

    @staticmethod
    def _to_table(data: pa.Table | pa.RecordBatch) -> pa.Table:
        if isinstance(data, pa.RecordBatch):
            return pa.Table.from_batches([data])
        if isinstance(data, pa.Table):
            return data
        raise TypeError(f"data must be pa.Table or pa.RecordBatch, got {type(data)}")

    @classmethod
    def _validate(cls, table: pa.Table) -> None:
        if cls.SYMBOL_COL not in table.schema.names:
            raise ValueError(f"missing required column: {cls.SYMBOL_COL}")

        # 契约要求 Symbol 为 string；若不是 string，直接失败（不在此阶段做映射/修复）
        field = table.schema.field(cls.SYMBOL_COL)
        if field.type != pa.string():
            raise TypeError(f"column '{cls.SYMBOL_COL}' must be string, got {field.type}")
