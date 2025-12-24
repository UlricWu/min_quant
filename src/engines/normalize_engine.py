from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.engines.parser_engine import parse_events_arrow
from src.meta.meta import MetaResult

from pathlib import Path
from functools import reduce
from typing import Dict, Tuple


class NormalizeEngine:
    """
    NormalizeEngine（冻结契约版）

    - 输入：交易所级 parquet
    - 输出：canonical parquet（symbol, ts 全量排序）
    - 计算 symbol slice index（不拆分）
    """

    VALID_EVENTS = {"ADD", "CANCEL", "TRADE"}
    batch_size = 50_000_000  # 5e7

    def execute(self, input_file: Path, output_dir: Path) -> MetaResult:
        exchange, kind = input_file.stem.split("_", 1)
        output_file = output_dir / input_file.name

        pf = pq.ParquetFile(input_file)
        tables: list[pa.Table] = []

        # --------------------------------------------------
        # 1. 读取 + 过滤 + parse（批次级）
        # --------------------------------------------------
        for batch in pf.iter_batches(self.batch_size):
            table = pa.Table.from_batches([batch])

            table = self.filter_a_share_arrow(table)
            if table.num_rows == 0:
                continue

            table = parse_events_arrow(
                table,
                exchange=exchange,
                kind=kind,
            )
            if table.num_rows == 0:
                continue

            tables.append(table)

        if not tables:
            # 空输出也要生成 parquet（但 index 为空）
            empty = pa.table({})
            pq.write_table(empty, output_file)
            return MetaResult(
                input_file=input_file,
                output_file=output_file,
                rows=0,
                index={},
            )

        # --------------------------------------------------
        # 2. 拼接 + 全量排序（关键）
        # --------------------------------------------------
        table = pa.concat_tables(tables, promote_options="default")

        sort_indices = pc.sort_indices(
            table,
            sort_keys=[
                ("symbol", "ascending"),
                ("ts", "ascending"),
            ],
        )
        table = table.take(sort_indices)

        # --------------------------------------------------
        # 3. 构建 symbol slice index（O(N)）
        # --------------------------------------------------
        symbol_col = table["symbol"]
        symbols = symbol_col.to_pylist()

        index: Dict[str, Tuple[int, int]] = {}

        start = 0
        current = symbols[0]

        for i in range(1, len(symbols)):
            if symbols[i] != current:
                index[current] = (start, i - start)
                current = symbols[i]
                start = i

        # 最后一个 symbol
        index[current] = (start, len(symbols) - start)

        # --------------------------------------------------
        # 4. 写 parquet（一次性）
        # --------------------------------------------------
        pq.write_table(table, output_file)

        # --------------------------------------------------
        # 5. 返回 NormalizeResult（最小完备）
        # --------------------------------------------------
        return MetaResult(
            input_file=input_file,
            output_file=output_file,
            rows=table.num_rows,
            index=index,
        )

    # --------------------------------------------------
    # A 股过滤（保持你原有逻辑）
    # --------------------------------------------------
    def filter_a_share_arrow(self, table: pa.Table) -> pa.Table:
        symbol = pc.cast(table["SecurityID"], pa.string())

        prefixes = [
            "60", "688",
            "00", "300",
        ]

        masks = [pc.starts_with(symbol, p) for p in prefixes]
        mask = reduce(pc.or_, masks)

        return table.filter(mask)

    @staticmethod
    def build_symbol_slice_index(sorted_table: pa.Table) -> Dict[str, Tuple[int, int]]:
        """
        Build symbol -> (start, length) without symbol_col.to_pylist().

        Preconditions:
          - sorted_table is globally sorted by ('symbol', 'ts') ascending
          - 'symbol' column exists and is not empty

        Complexity:
          - Arrow C++ O(N) for run-end encoding
          - Python loop over #symbols (typically a few thousand)
        """
        if sorted_table.num_rows == 0:
            return {}

        sym = sorted_table["symbol"]

        # Ensure symbol is a simple type (string or dictionary<string>)
        # RLE works well on dictionary too; but casting to string is OK if needed.
        if not pa.types.is_string(sym.type) and not pa.types.is_dictionary(sym.type):
            sym = pc.cast(sym, pa.string())

        # run_end_encode returns a StructArray with fields: 'values' and 'run_ends'
        # run_ends are 1-based end positions (exclusive end indices).
        ree = pc.run_end_encode(sym)

        values = ree.field("values")
        run_ends = ree.field("run_ends")  # int32/int64 array

        # Convert ONLY per-symbol arrays to python lists (few thousand items)
        vals_py = values.to_pylist()
        ends_py = run_ends.to_pylist()

        index: Dict[str, Tuple[int, int]] = {}
        start = 0
        for v, end_exclusive in zip(vals_py, ends_py):
            length = int(end_exclusive) - start
            index[str(v)] = (start, length)
            start = int(end_exclusive)

        return index
