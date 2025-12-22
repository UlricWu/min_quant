from typing import Literal
from pathlib import Path
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from functools import reduce

from src.engines.parser_engine import parse_events_arrow

class NormalizeEngine:
    """
    NormalizeEngine（冻结契约版）

    - 输入：交易所级 parquet
    - 输出：canonical order / trade parquet
    - symbol 只是字段，不做拆分
    """

    VALID_EVENTS = {"ADD", "CANCEL", "TRADE"}
    batch_size = 5_000_000_0

    def execute(self, input_file: Path, output_dir: Path) -> None:
        exchange, kind = input_file.stem.split("_", 1)
        out_path = output_dir / input_file.name

        pf = pq.ParquetFile(input_file)
        writer = None

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
            if writer is None:
                writer = pq.ParquetWriter(out_path, table.schema)

            writer.write_table(table)

        if writer:
            writer.close()

    def filter_a_share_arrow(self, table: pa.Table) -> pa.Table:
        symbol = pc.cast(table["SecurityID"], pa.string())

        # prefixes = [
        #     "600", "601", "603", "605", "688",
        #     "000", "001", "002", "003", "300",
        # ]
        prefixes = [
            "60", "688",
            "00", "300",
        ]

        masks = [pc.starts_with(symbol, p) for p in prefixes]

        mask = reduce(pc.or_, masks)

        return table.filter(mask)