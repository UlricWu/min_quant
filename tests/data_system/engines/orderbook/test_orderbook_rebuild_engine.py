from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from src.data_system.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.data_system.engines.context import EngineContext


from pathlib import Path

import pytest


# ------------------------------------------------------------
# helper: 写 order / trade 事件 parquet
# ------------------------------------------------------------
def write_order_events(path, rows):
    schema = pa.schema(
        [
            ("ts", pa.int64()),
            ("event", pa.string()),  # ADD / CANCEL / TRADE
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
            input_file=in_path,
            output_file=out_path,
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
            input_file=in_path,
            output_file=out_path,
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
            input_file=in_path,
            output_file=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()

    bid = df.iloc[0]
    assert bid["price"] == 10.0
    assert bid["volume"] == 60  # 100 - 40


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
            input_file=in_path,
            output_file=out_path,
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
            input_file=in_path,
            output_file=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()
    bids = df[df["side"] == "B"].sort_values("level")

    assert bids.iloc[0]["price"] == 11.0  # best bid
    assert bids.iloc[1]["price"] == 10.0
    assert bids.iloc[2]["price"] == 9.0


@pytest.fixture
def canonical_events_parquet(tmp_path: Path) -> Path:
    table = pa.table(
        {
            "ts": [1, 2, 3, 4, 5],
            "event": ["ADD", "ADD", "CANCEL", "ADD", "TRADE"],
            "order_id": [1, 2, 1, 3, 2],
            "side": ["B", "S", "B", "B", "S"],
            "price": [10.0, 10.5, 10.0, 9.8, 10.5],
            "volume": [100, 200, 100, 50, 200],
        },
        schema=pa.schema(
            [
                ("ts", pa.int64()),
                ("event", pa.string()),
                ("order_id", pa.int64()),
                ("side", pa.string()),
                ("price", pa.float64()),
                ("volume", pa.int64()),
            ]
        ),
    )

    path = tmp_path / "canonical.parquet"
    pq.write_table(table, path)
    return path


def test_orderbook_rebuild_offline_basic(tmp_path: Path, canonical_events_parquet: Path):
    out_snapshot = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine(record_events=False)

    ctx = EngineContext(
        mode="offline",
        input_file=canonical_events_parquet,
        output_file=out_snapshot,
    )

    engine.execute(ctx)

    assert out_snapshot.exists()

    snapshot = pq.read_table(out_snapshot)
    data = snapshot.to_pydict()

    # -------------------------
    # 校验 schema
    # -------------------------
    assert snapshot.schema.names == [
        "ts", "side", "level", "price", "volume"
    ]

    # -------------------------
    # 校验盘口结果
    # -------------------------
    # 只剩一个买盘
    assert data["side"] == ["B"]
    assert data["level"] == [1]
    assert data["price"] == [9.8]
    assert data["volume"] == [50]

    # ts = 最后事件 ts
    assert data["ts"] == [5]


def test_orderbook_price_sorting(tmp_path: Path):
    table = pa.table(
        {
            "ts": [1, 2, 3],
            "event": ["ADD", "ADD", "ADD"],
            "order_id": [1, 2, 3],
            "side": ["B", "B", "S"],
            "price": [9.9, 10.1, 10.5],
            "volume": [100, 200, 300],
        }
    )

    input_path = tmp_path / "events.parquet"
    pq.write_table(table, input_path)

    out = tmp_path / "snapshot.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
        )
    )

    snap = pq.read_table(out).to_pydict()

    # BID: 10.1 -> 9.9
    assert snap["side"][:2] == ["B", "B"]
    assert snap["price"][:2] == [10.1, 9.9]
    assert snap["volume"][:2] == [200, 100]

    # ASK
    assert snap["side"][2:] == ["S"]
    assert snap["price"][2:] == [10.5]


def test_orderbook_empty_input(tmp_path: Path):
    empty = pa.table(
        {
            "ts": pa.array([], pa.int64()),
            "event": pa.array([], pa.string()),
            "order_id": pa.array([], pa.int64()),
            "side": pa.array([], pa.string()),
            "price": pa.array([], pa.float64()),
            "volume": pa.array([], pa.int64()),
        }
    )

    input_path = tmp_path / "empty.parquet"
    pq.write_table(empty, input_path)

    out = tmp_path / "snapshot.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
        )
    )

    snap = pq.read_table(out)
    assert snap.num_rows == 0
