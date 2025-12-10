#!filepath: src/l2/offline/parquet_event_source.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

import pyarrow.dataset as ds

from src import logs
from src.l2.engine.events import TradeEvent


@dataclass
class TradeParquetSchema:
    """
    描述 parquet 中字段名映射关系：
    - 默认假设：
        ts_ns_col      = "ts_ns"
        price_col      = "Price"
        qty_col        = "Qty"
        security_id_col= "SecurityID"
    你可以根据实际字段修改。
    """
    ts_ns_col: str = "ts_ns"
    price_col: str = "Price"
    qty_col: str = "Qty"
    security_id_col: str = "SecurityID"


class ParquetTradeEventSource:
    """
    将 symbol 级 Trade.parquet 以 streaming 方式转为 TradeEvent 流。
    """

    def __init__(self, parquet_path: Path, schema: TradeParquetSchema | None = None) -> None:
        self.parquet_path = parquet_path
        self.schema = schema or TradeParquetSchema()

    def iter_events(self) -> Iterator[TradeEvent]:
        if not self.parquet_path.exists():
            raise FileNotFoundError(f"Trade parquet 不存在: {self.parquet_path}")

        logs.info(f"[ParquetTradeEventSource] 读取: {self.parquet_path}")

        dataset = ds.dataset(self.parquet_path, format="parquet")

        for batch in dataset.to_batches():
            if batch.num_rows == 0:
                continue

            cols = batch.schema.names
            s = self.schema

            for col in (s.ts_ns_col, s.price_col, s.qty_col, s.security_id_col):
                if col not in cols:
                    raise KeyError(f"parquet 中缺少列: {col}")

            ts_arr = batch.column(cols.index(s.ts_ns_col)).cast("int64")
            px_arr = batch.column(cols.index(s.price_col)).cast("float64")
            qty_arr = batch.column(cols.index(s.qty_col)).cast("int64")
            sec_arr = batch.column(cols.index(s.security_id_col))

            ts_list = ts_arr.to_pylist()
            px_list = px_arr.to_pylist()
            qty_list = qty_arr.to_pylist()
            sec_list = sec_arr.to_pylist()

            for ts_ns, px, qty, sec in zip(ts_list, px_list, qty_list, sec_list):
                if ts_ns is None or sec is None:
                    continue
                yield TradeEvent(
                    ts_ns=int(ts_ns),
                    price=float(px),
                    qty=int(qty),
                    security_id=str(sec),
                )
