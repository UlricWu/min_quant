#!filepath: src/engines/normalize_engine.py
from __future__ import annotations
from functools import reduce
from pathlib import Path
from typing import List
import pyarrow.compute as pc
import pandas as pd
from dataclasses import dataclass, asdict

from src.engines.context import EngineContext
from src.l2.common.event_parser import parse_events, EventKind
from src import logs
import pyarrow as pa
import pyarrow.parquet as pq
from src.utils.parquet_writer import ParquetAppendWriter
ALLOWED_SYMBOL_PREFIXES = {
    "600", "601", "603", "605",
    "688",
    "000", "001", "002", "003",
    "300",
}

CANONICAL_SCHEMA = pa.schema([
    ("symbol", pa.string()),
    ("ts", pa.int64()),
    ("event", pa.string()),
    ("order_id", pa.int64()),
    ("side", pa.string()),
    ("price", pa.float64()),
    ("volume", pa.int64()),
    ("buy_no", pa.int64()),
    ("sell_no", pa.int64()),
])


def is_valid_a_share_symbol(symbol: str) -> bool:
    return (
            symbol.isdigit()
            and symbol[:3] in ALLOWED_SYMBOL_PREFIXES
    )


@dataclass(slots=True)
class NormalizedEvent:
    symbol: str
    ts: int
    event: str
    order_id: int
    side: str | None
    price: float
    volume: int
    buy_no: int
    sell_no: int

    @classmethod
    def from_row(cls, row):
        return cls(
            symbol=str(row.symbol),
            ts=int(row.ts),
            event=row.event,
            order_id=int(row.order_id),
            side=None if row.side != row.side else row.side,
            price=float(row.price),
            volume=int(row.volume),
            buy_no=int(row.buy_no),
            sell_no=int(row.sell_no),
        )

    def to_dict(self) -> dict:
        return asdict(self)


class NormalizeEngine:
    """
    NormalizeEngine（冻结契约版）

    - 输入：交易所级 parquet
    - 输出：canonical order / trade parquet
    - symbol 只是字段，不做拆分
    """

    VALID_EVENTS = {"ADD", "CANCEL", "TRADE"}

    def execute(self, ctx: EngineContext) -> None:
        assert ctx.mode == "offline"
        in_dir = ctx.input_path
        out_dir = ctx.output_path

        for kind in ("order", "trade"):
            self._normalize_kind(kind=kind, in_dir=in_dir, out_dir=out_dir)

    def _normalize_kind(
            self,
            *,
            kind: EventKind,
            in_dir: Path,
            out_dir: Path,
    ) -> None:
        for path in sorted(in_dir.glob(f"*_{kind.capitalize()}.parquet")):
            exchange = path.name.split("_")[0]

            out_path = out_dir / f"{exchange}_{kind.capitalize()}.parquet"
            writer = ParquetAppendWriter(out_path, CANONICAL_SCHEMA)

            pf = pq.ParquetFile(path)

            for batch in pf.iter_batches(batch_size=1_000_0000):
                table = pa.Table.from_batches([batch])
                # ✅ 在这里做 A 股过滤（Arrow）
                table = filter_a_share_arrow(table)
                if table.num_rows == 0:
                    continue
                # Arrow → pandas（单批）
                df = table.to_pandas(types_mapper=pd.ArrowDtype)

                if df.empty:
                    continue

                norm = parse_events(df, kind=kind)
                if norm.empty:
                    continue

                norm = norm[norm["event"].isin(self.VALID_EVENTS)]
                if norm.empty:
                    continue

                # norm["symbol"] = norm["symbol"].astype(str).str.zfill(6)
                norm = norm[norm["symbol"].apply(is_valid_a_share_symbol)]
                if norm.empty:
                    continue
                logs.debug(f"[Normalize] start batch rows={len(df)}")
                norm.sort_values("ts", inplace=True)

                # ⚠️ 关键：直接 DataFrame → Arrow Table
                table = pa.Table.from_pandas(
                    norm[
                        [
                            "symbol",
                            "ts",
                            "event",
                            "order_id",
                            "side",
                            "price",
                            "volume",
                            "buy_no",
                            "sell_no",
                        ]
                    ],
                    schema=CANONICAL_SCHEMA,
                    preserve_index=False,
                )

                writer.write(table)

            writer.close()
def filter_a_share_arrow(table: pa.Table) -> pa.Table:
    symbol = pc.cast(table["SecurityID"], pa.string())

    prefixes = [
        "600", "601", "603", "605", "688",
        "000", "001", "002", "003", "300",
    ]

    masks = [pc.starts_with(symbol, p) for p in prefixes]

    mask = reduce(pc.or_, masks)

    return table.filter(mask)