from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from src.data_system.engines.minute_order_agg_engine import MinuteOrderAggEngine
from src.data_system.engines.context import EngineContext

US_PER_MINUTE = 60 * 1_000_000


def test_minute_order_agg_basic(tmp_path, write_parquet):
    """
    单 symbol / 单 minute / ADD + CANCEL
    """
    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"

    base_ts = 10 * US_PER_MINUTE

    rows = [
        {
            "ts": base_ts + 1,
            "event": "ADD",
            "order_id": 1,
            "side": "B",
            "price": 10.0,
            "volume": 100,
            "notional": 1000.0,
        },
        {
            "ts": base_ts + 2,
            "event": "CANCEL",
            "order_id": 1,
            "side": "B",
            "price": 10.0,
            "volume": 40,
            "notional": 400.0,
        },
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

    engine = MinuteOrderAggEngine()
    ctx = EngineContext(
        mode="offline",
        input_file=in_path,
        output_file=out_path,
    )

    engine.execute(ctx)

    assert out_path.exists()

    table = pq.read_table(out_path)
    df = table.to_pandas()

    assert len(df) == 1
    r = df.iloc[0]

    assert r["add_volume"] == 100
    assert r["cancel_volume"] == 40
    assert r["net_volume"] == 60

    assert r["add_notional"] == 1000.0
    assert r["cancel_notional"] == 400.0
    assert r["event_count"] == 2

def test_minute_order_agg_multi_minute(tmp_path, write_parquet):
    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"

    rows = [
        {
            "ts": 1 * US_PER_MINUTE + 1,
            "event": "ADD",
            "order_id": 1,
            "side": "B",
            "price": 10.0,
            "volume": 100,
            "notional": 1000.0,
        },
        {
            "ts": 2 * US_PER_MINUTE + 1,
            "event": "ADD",
            "order_id": 2,
            "side": "S",
            "price": 11.0,
            "volume": 50,
            "notional": 550.0,
        },
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

    engine = MinuteOrderAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=in_path,
            output_file=out_path,
            key=''
        )
    )

    df = pq.read_table(out_path).to_pandas()
    assert len(df) == 2
# ------------------------------------------------------------
# helper: 写 orderbook_events.parquet
# ------------------------------------------------------------
def write_orderbook_events(path, rows):
    schema = pa.schema(
        [
            ("ts", pa.int64()),          # us
            ("event", pa.string()),      # ADD / CANCEL
            ("order_id", pa.int64()),
            ("side", pa.string()),
            ("price", pa.float64()),
            ("volume", pa.int64()),
            ("notional", pa.float64()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)
# ============================================================
# 2. 跨 minute
# ============================================================
def test_minute_order_agg_multi_minute(tmp_path):
    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"

    rows = [
        {
            "ts": 1 * US_PER_MINUTE + 10,
            "event": "ADD",
            "order_id": 1,
            "side": "B",
            "price": 10.0,
            "volume": 100,
            "notional": 1000.0,
        },
        {
            "ts": 2 * US_PER_MINUTE + 20,
            "event": "ADD",
            "order_id": 2,
            "side": "S",
            "price": 20.0,
            "volume": 50,
            "notional": 1000.0,
        },
    ]

    write_orderbook_events(in_path, rows)

    engine = MinuteOrderAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=in_path,
            output_file=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()

    assert len(df) == 2

    minutes = sorted(df["minute"].astype("int64").tolist())
    assert minutes[0] == 1 * US_PER_MINUTE
    assert minutes[1] == 2 * US_PER_MINUTE
# ============================================================
# 3. 只有 ADD（无 CANCEL）
# ============================================================
def test_minute_order_agg_only_add(tmp_path):
    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"

    rows = [
        {
            "ts": 0,
            "event": "ADD",
            "order_id": 1,
            "side": "B",
            "price": 10.0,
            "volume": 100,
            "notional": 1000.0,
        }
    ]

    write_orderbook_events(in_path, rows)

    engine = MinuteOrderAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=in_path,
            output_file=out_path,

        )
    )

    df = pq.read_table(out_path).to_pandas()
    r = df.iloc[0]

    assert r["add_volume"] == 100
    assert r["cancel_volume"] == 0
    assert r["net_volume"] == 100
    assert r["event_count"] == 1
# ============================================================
# 4. 空输入（size > 0 但无 rows）
# ============================================================
def test_minute_order_agg_empty_input(tmp_path):
    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"

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

    empty_table = pa.Table.from_arrays(
        [pa.array([], type=f.type) for f in schema],
        schema=schema,
    )
    pq.write_table(empty_table, in_path)

    engine = MinuteOrderAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=in_path,
            output_file=out_path,
        )
    )

    assert not out_path.exists() or pq.read_table(out_path).num_rows == 0

# ============================================================
# 5. schema & 列名固定（防回归）
# ============================================================
def test_minute_order_agg_schema(tmp_path):
    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"

    rows = [
        {
            "ts": 0,
            "event": "ADD",
            "order_id": 1,
            "side": "B",
            "price": 1.0,
            "volume": 1,
            "notional": 1.0,
        }
    ]

    write_orderbook_events(in_path, rows)

    engine = MinuteOrderAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=in_path,
            output_file=out_path,
        )
    )

    table = pq.read_table(out_path)
    assert table.schema.names == ['minute', 'add_volume', 'cancel_volume', 'net_volume', 'add_notional', 'cancel_notional', 'event_count']