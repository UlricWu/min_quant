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
