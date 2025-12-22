from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.pipeline.context import EngineContext


# ------------------------------------------------------------
# helper: 写 order / trade 事件 parquet
# ------------------------------------------------------------
def write_order_events(path, rows):
    schema = pa.schema(
        [
            ("ts", pa.int64()),
            ("event", pa.string()),       # ADD / CANCEL / TRADE
            ("order_id", pa.int64()),
            ("side", pa.string()),
            ("price", pa.float64()),
            ("volume", pa.int64()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


# ============================================================
# 1. ADD → snapshot
# ============================================================
def test_orderbook_add_only(tmp_path):
    in_path = tmp_path / "events.parquet"
    out_path = tmp_path / "orderbook.parquet"

    rows = [
        {
            "ts": 1,
            "event": "ADD",
            "order_id": 1,
            "side": "B",
            "price": 10.0,
            "volume": 100,
        },
        {
            "ts": 2,
            "event": "ADD",
            "order_id": 2,
            "side": "S",
            "price": 11.0,
            "volume": 50,
        },
    ]

    write_order_events(in_path, rows)

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()

    # 买一
    bid = df[df["side"] == "B"].iloc[0]
    assert bid["price"] == 10.0
    assert bid["volume"] == 100
    assert bid["level"] == 1

    # 卖一
    ask = df[df["side"] == "S"].iloc[0]
    assert ask["price"] == 11.0
    assert ask["volume"] == 50
    assert ask["level"] == 1


# ============================================================
# 2. ADD → CANCEL
# ============================================================
def test_orderbook_cancel(tmp_path):
    in_path = tmp_path / "events.parquet"
    out_path = tmp_path / "orderbook.parquet"

    rows = [
        {"ts": 1, "event": "ADD", "order_id": 1, "side": "B", "price": 10.0, "volume": 100},
        {"ts": 2, "event": "CANCEL", "order_id": 1, "side": "B", "price": 10.0, "volume": 100},
    ]

    write_order_events(in_path, rows)

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    table = pq.read_table(out_path)
    assert table.num_rows == 0


# ============================================================
# 3. ADD → TRADE (partial)
# ============================================================
def test_orderbook_partial_trade(tmp_path):
    in_path = tmp_path / "events.parquet"
    out_path = tmp_path / "orderbook.parquet"

    rows = [
        {"ts": 1, "event": "ADD", "order_id": 1, "side": "B", "price": 10.0, "volume": 100},
        {"ts": 2, "event": "TRADE", "order_id": 1, "side": "B", "price": 10.0, "volume": 40},
    ]

    write_order_events(in_path, rows)

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()

    bid = df.iloc[0]
    assert bid["price"] == 10.0
    assert bid["volume"] == 60   # 100 - 40


# ============================================================
# 4. ADD → TRADE (full fill)
# ============================================================
def test_orderbook_full_trade(tmp_path):
    in_path = tmp_path / "events.parquet"
    out_path = tmp_path / "orderbook.parquet"

    rows = [
        {"ts": 1, "event": "ADD", "order_id": 1, "side": "B", "price": 10.0, "volume": 100},
        {"ts": 2, "event": "TRADE", "order_id": 1, "side": "B", "price": 10.0, "volume": 100},
    ]

    write_order_events(in_path, rows)

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    table = pq.read_table(out_path)
    assert table.num_rows == 0


# ============================================================
# 5. 多档位 / 排序正确
# ============================================================
def test_orderbook_multi_level_sorting(tmp_path):
    in_path = tmp_path / "events.parquet"
    out_path = tmp_path / "orderbook.parquet"

    rows = [
        {"ts": 1, "event": "ADD", "order_id": 1, "side": "B", "price": 10.0, "volume": 100},
        {"ts": 2, "event": "ADD", "order_id": 2, "side": "B", "price": 11.0, "volume": 50},
        {"ts": 3, "event": "ADD", "order_id": 3, "side": "B", "price": 9.0, "volume": 30},
    ]

    write_order_events(in_path, rows)

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()
    bids = df[df["side"] == "B"].sort_values("level")

    assert bids.iloc[0]["price"] == 11.0  # best bid
    assert bids.iloc[1]["price"] == 10.0
    assert bids.iloc[2]["price"] == 9.0
