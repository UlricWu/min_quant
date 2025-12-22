from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from src.pipeline.context import PipelineContext
from src.engines.minute_order_agg_engine import MinuteOrderAggEngine
from src.steps.minute_order_agg_step import MinuteOrderAggStep

from contextlib import contextmanager

class DummyInst:
    @contextmanager
    def timer(self, name):
        yield
def test_step_skips_missing_input(tmp_path):
    symbol_dir = tmp_path / "symbol" / "000001"
    symbol_dir.mkdir(parents=True)

    ctx = PipelineContext(
        date="2015-01-01",
        symbol_dir=tmp_path / "symbol",
        raw_dir='',
        parquet_dir='',
        canonical_dir='',
        meta_dir=''

    )

    step = MinuteOrderAggStep(MinuteOrderAggEngine(),inst=DummyInst())
    step.run(ctx)

    assert not (symbol_dir / "minute_order.parquet").exists()


def test_step_runs_successfully(tmp_path, write_parquet):
    symbol_root = tmp_path / "symbol"
    sym_dir = symbol_root / "000001"
    sym_dir.mkdir(parents=True)

    in_path = sym_dir / "orderbook_events.parquet"
    out_path = sym_dir / "minute_order.parquet"

    rows = [
        {
            "ts": 0,
            "event": "ADD",
            "order_id": 1,
            "side": "B",
            "price": 10.0,
            "volume": 10,
            "notional": 100.0,
        }
    ]

    schema = pa.schema(
        [
            ("ts", pa.int64()),
            ("event", pa.string()),
            ("order_id", pa.int64()),
            ("side", pa.string()),
            ("price", pa.float64()),
            ("volume", pa.int64()),
            ("notional", pa.float64()),
        ]
    )

    write_parquet(in_path, rows, schema)

    ctx = PipelineContext(
        date="2015-01-01",
        symbol_dir=symbol_root,
        raw_dir='',
        parquet_dir='',
        canonical_dir='',
        meta_dir=''
    )

    step = MinuteOrderAggStep(MinuteOrderAggEngine(),inst=DummyInst())
    step.run(ctx)

    assert out_path.exists()
