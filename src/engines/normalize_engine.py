#!filepath: src/engines/normalize_engine.py
from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Dict, Iterable, Tuple

import pyarrow as pa
import pyarrow.compute as pc


# -----------------------------------------------------------------------------
# Pure result (Engine only returns data artifacts)
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class NormalizeResult:
    """
    NormalizeResult（纯计算输出）

    canonical:
      - (symbol asc, ts asc) 全量排序后的 Arrow Table

    index:
      - symbol -> (start, length) slice 索引（end-exclusive）

    rows:
      - canonical 行数（方便上层统计/写 meta）
    """
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

# from dataclasses import dataclass
# from pathlib import Path
# from typing import Dict, Tuple
#
# import pyarrow as pa
# import pyarrow.compute as pc
# import pyarrow.parquet as pq
#
# from src.engines.parser_engine import parse_events_arrow
# from src.meta.meta import MetaResult
#
# from pathlib import Path
# from functools import reduce
# from typing import Dict, Tuple
#
#
# class NormalizeEngine:
#     """
#     NormalizeEngine（冻结契约版）
#
#     - 输入：交易所级 parquet
#     - 输出：canonical parquet（symbol, ts 全量排序）
#     - 计算 symbol slice index（不拆分）
#     """
#
#     VALID_EVENTS = {"ADD", "CANCEL", "TRADE"}
#     batch_size = 50_000_000  # 5e7
#
#     def execute(self, input_file: Path, output_dir: Path) -> MetaResult:
#         exchange, kind = input_file.stem.split("_", 1)
#         output_file = output_dir / input_file.name
#
#         pf = pq.ParquetFile(input_file)
#         tables: list[pa.Table] = []
#
#         # --------------------------------------------------
#         # 1. 读取 + 过滤 + parse（批次级）
#         # --------------------------------------------------
#         for batch in pf.iter_batches(self.batch_size):
#             table = pa.Table.from_batches([batch])
#
#             table = self.filter_a_share_arrow(table)
#             if table.num_rows == 0:
#                 continue
#
#             table = parse_events_arrow(
#                 table,
#                 exchange=exchange,
#                 kind=kind,
#             )
#             if table.num_rows == 0:
#                 continue
#
#             tables.append(table)
#
#         if not tables:
#             # 空输出也要生成 parquet（但 index 为空）
#             empty = pa.table({})
#             pq.write_table(empty, output_file)
#             return MetaResult(
#                 input_file=input_file,
#                 output_file=output_file,
#                 rows=0,
#                 index={},
#             )
#
#         # --------------------------------------------------
#         # 2. 拼接 + 全量排序（关键）
#         # --------------------------------------------------
#         table = pa.concat_tables(tables, promote_options="default")
#
#         sort_indices = pc.sort_indices(
#             table,
#             sort_keys=[
#                 ("symbol", "ascending"),
#                 ("ts", "ascending"),
#             ],
#         )
#         table = table.take(sort_indices)
#
#         # # --------------------------------------------------
#         # # 3. 构建 symbol slice index（O(N)）
#         # # --------------------------------------------------
#         # symbol_col = table["symbol"]
#         # symbols = symbol_col.to_pylist()
#         #
#         # index: Dict[str, Tuple[int, int]] = {}
#         #
#         # start = 0
#         # current = symbols[0]
#         #
#         # for i in range(1, len(symbols)):
#         #     if symbols[i] != current:
#         #         index[current] = (start, i - start)
#         #         current = symbols[i]
#         #         start = i
#         #
#         # # 最后一个 symbol
#         # index[current] = (start, len(symbols) - start)
#         index = self.build_symbol_slice_index(table)
#
#         # --------------------------------------------------
#         # 4. 写 parquet（一次性）
#         # --------------------------------------------------
#         pq.write_table(table, output_file)
#
#         # --------------------------------------------------
#         # 5. 返回 NormalizeResult（最小完备）
#         # --------------------------------------------------
#         return MetaResult(
#             input_file=input_file,
#             output_file=output_file,
#             rows=table.num_rows,
#             index=index,
#         )
#
#     # --------------------------------------------------
#     # A 股过滤（保持你原有逻辑）
#     # --------------------------------------------------
#     def filter_a_share_arrow(self, table: pa.Table) -> pa.Table:
#         symbol = pc.cast(table["SecurityID"], pa.string())
#
#         prefixes = [
#             "60", "688",
#             "00", "300",
#         ]
#
#         masks = [pc.starts_with(symbol, p) for p in prefixes]
#         mask = reduce(pc.or_, masks)
#
#         return table.filter(mask)
#
#     @staticmethod
#     def build_symbol_slice_index(sorted_table: pa.Table) -> Dict[str, Tuple[int, int]]:
#         if sorted_table.num_rows == 0:
#             return {}
#
#         sym = sorted_table["symbol"]
#
#         if not pa.types.is_string(sym.type) and not pa.types.is_dictionary(sym.type):
#             sym = pc.cast(sym, pa.string())
#
#         ree = pc.run_end_encode(sym)
#         single_array = ree.combine_chunks()
#         run_ends = single_array.run_ends
#         run_values = single_array.values
#         # Convert ONLY per-symbol info to Python
#         values_py = run_values.to_pylist()
#         run_ends_py = run_ends.to_pylist()
#         index: Dict[str, Tuple[int, int]] = {}
#         start = 0
#
#         for sym_val, end_exclusive in zip(values_py, run_ends_py):
#             end_exclusive = int(end_exclusive)
#             index[str(sym_val)] = (start, end_exclusive - start)
#             start = end_exclusive
#
#         return index
