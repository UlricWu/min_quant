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

    SYMBOL_COL = "Symbol" # SecurityID

    def split(self, data: pa.Table | pa.RecordBatch) -> Dict[str, pa.Table]:
        table = self._to_table(data)
        self._validate(table)

        sym_arr = table[self.SYMBOL_COL]

        # 确定性：按首次出现顺序枚举 unique symbols
        unique_symbols = self._unique_in_appearance_order(sym_arr)

        out: Dict[str, pa.Table] = {}
        for sym in unique_symbols:
            # sym 可能为 None（缺失），跳过（不会产生文件）
            if sym is None:
                continue
            mask = pc.equal(sym_arr, pa.scalar(sym))
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

    @staticmethod
    def _unique_in_appearance_order(arr: pa.ChunkedArray) -> list[str | None]:
        seen: set[str | None] = set()
        uniq: list[str | None] = []
        for v in arr.to_pylist():
            if v not in seen:
                seen.add(v)
                uniq.append(v)
        return uniq

# from __future__ import annotations
#
# from pathlib import Path
# from typing import Iterable
#
# import pyarrow as pa
# import pyarrow.compute as pc
# import pyarrow.dataset as ds
# import pyarrow.parquet as pq
#
# from src.utils.filesystem import FileSystem
# from src import logs
#
#
# class SymbolRouterEngine:
#     """
#     执行引擎：按 symbol 拆分 parquet 文件。
#
#     职责：
#     - 打开 parquet dataset
#     - 流式读取 RecordBatch
#     - 按 symbol 写出子 parquet
#
#     不关心：
#     - symbols 从哪里来
#     - 是否 skip
#     - 时间语义 / instrumentation
#     """
#
#     def route_file(
#             self,
#             *,
#             date: str,
#             kind: str,
#             parquet_path: Path,
#             symbol_dir: Path,
#             symbols: set[str],
#     ) -> None:
#
#         dataset = ds.dataset(parquet_path)
#         writers: dict[str, pq.ParquetWriter] = {}
#         logs.info(f'start parquet file: {parquet_path.name}')
#         for batch in dataset.to_batches():
#             # SecurityID -> string（已经是 6 位）
#             sym_arr = batch["SecurityID"].cast(pa.string())
#
#             # 🔥 只取 batch 中真实存在的 symbol
#             unique_syms = pc.unique(sym_arr).to_pylist()
#
#             for sym in unique_syms:
#                 if sym is None:
#                     continue
#
#                 # 只处理关心的 symbols
#                 if sym not in symbols:
#                     continue
#
#                 mask = pc.equal(sym_arr, sym)
#                 sub = batch.filter(mask)
#                 if sub.num_rows == 0:
#                     continue
#
#                 writer = writers.get(sym)
#                 if writer is None:
#                     out = symbol_dir / sym / date
#                     FileSystem.ensure_dir(out)
#
#                     if kind.lower().endswith("order"):
#                         out_parquet = out / "Order.parquet"
#                     elif kind.lower().endswith("trade"):
#                         out_parquet = out / "Trade.parquet"
#                     else:
#                         raise ValueError(f"Unknown kind: {kind}")
#
#                     writer = pq.ParquetWriter(out_parquet, sub.schema)
#                     writers[sym] = writer
#
#                 writer.write_batch(sub)
#
#         for w in writers.values():
#             w.close()
