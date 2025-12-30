#!filepath: src/engines/normalize_engine.py
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Dict, Iterable, Tuple

import pyarrow as pa
import pyarrow.compute as pc

# ----------------------------------------------------------------------
# Result (pure data)
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class NormalizeResult:
    canonical: pa.Table
    index: Dict[str, Tuple[int, int]]
    rows: int


class NormalizeEngine:
    """
    NormalizeEngine（v2 · 纯 Arrow 冻结版）

    输入：
      - Iterable[pa.Table]：由 Step 读取 parquet / 批次处理 / parse 后提供
        （即：输入 tables 已经拥有至少 symbol, ts 两列）

    输出：
      - NormalizeResult（纯数据产物）
        - canonical table（排序后）
        - symbol slice index

    禁止：
      - 任何 I/O（不读 parquet，不写 parquet，不接触 Path）
      - 不 parse
      - 不返回 MetaResult
    """

    VALID_EVENTS = {"ADD", "CANCEL", "TRADE"}

    # --------------------------------------------------
    def execute(self, tables: Iterable[pa.Table]) -> NormalizeResult:
        tbls = [t for t in tables if t is not None and t.num_rows > 0]

        if not tbls:
            empty = pa.table({})
            return NormalizeResult(canonical=empty, index={}, rows=0)

        # --------------------------------------------------
        # 1) concat
        # --------------------------------------------------
        table = pa.concat_tables(tbls, promote_options="default")
        if table.num_rows == 0:
            empty = pa.table({})
            return NormalizeResult(canonical=empty, index={}, rows=0)

        # --------------------------------------------------
        # 2) validate (minimal contract)
        # --------------------------------------------------
        self._validate_required_columns(table)

        # --------------------------------------------------
        # 3) canonical sort: (symbol, ts)
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
        # 4) build symbol slice index (O(num_symbols))
        # --------------------------------------------------
        index = self.build_symbol_slice_index(table)

        return NormalizeResult(
            canonical=table,
            index=index,
            rows=table.num_rows,
        )

    # --------------------------------------------------
    @staticmethod
    def _validate_required_columns(table: pa.Table) -> None:
        missing = [c for c in ("symbol", "ts") if c not in table.column_names]
        if missing:
            raise ValueError(
                f"[NormalizeEngine] missing required columns: {missing}, "
                f"have={table.column_names}"
            )

        sym = table["symbol"]
        if not (pa.types.is_string(sym.type) or pa.types.is_dictionary(sym.type)):
            raise TypeError(
                f"[NormalizeEngine] column 'symbol' must be string/dictionary, got {sym.type}"
            )

        ts = table["ts"]
        if not (pa.types.is_integer(ts.type) or pa.types.is_timestamp(ts.type)):
            raise TypeError(
                f"[NormalizeEngine] column 'ts' must be integer/timestamp, got {ts.type}"
            )

    # --------------------------------------------------
    # A 股过滤（纯逻辑，允许给 Step/worker 调用）
    # --------------------------------------------------
    def filter_a_share_arrow(self, table: pa.Table) -> pa.Table:
        if table.num_rows == 0:
            return table

        if "SecurityID" not in table.column_names:
            raise ValueError("[NormalizeEngine] missing column: SecurityID")

        symbol = pc.cast(table["SecurityID"], pa.string())

        prefixes = [
            "60", "688",
            "00", "300",
        ]

        masks = [pc.starts_with(symbol, p) for p in prefixes]
        mask = reduce(pc.or_, masks)

        return table.filter(mask)

    # --------------------------------------------------
    @staticmethod
    def build_symbol_slice_index(sorted_table: pa.Table) -> Dict[str, Tuple[int, int]]:
        """
        Precondition:
          - sorted_table 已按 (symbol asc, ts asc) 排序
        """
        if sorted_table.num_rows == 0:
            return {}

        sym = sorted_table["symbol"]

        # keep strict: allow string/dictionary; cast dictionary to string for stable python conversion
        if pa.types.is_dictionary(sym.type):
            sym = pc.cast(sym, pa.string())
        elif not pa.types.is_string(sym.type):
            raise TypeError(f"[NormalizeEngine] invalid symbol type: {sym.type}")

        ree = pc.run_end_encode(sym).combine_chunks()
        run_ends_py = ree.run_ends.to_pylist()
        values_py = ree.values.to_pylist()

        index: Dict[str, Tuple[int, int]] = {}
        start = 0
        for sym_val, end_exclusive in zip(values_py, run_ends_py):
            end_exclusive = int(end_exclusive)
            index[str(sym_val)] = (start, end_exclusive - start)
            start = end_exclusive

        return index