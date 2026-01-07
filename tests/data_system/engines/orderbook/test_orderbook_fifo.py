from src.data_system.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.data_system.engines.context import EngineContext

import pyarrow.parquet as pq

"""
同价位 FIFO 
FIFO + 部分成交穿透
多价位排序正确性（B 高→低 / S 低→高）
"""
def test_fifo_same_price_multiple_orders(tmp_path, make_events_parquet):
    """
    同价位多个订单，必须严格 FIFO：
        ADD(1) → ADD(2) → TRADE(1)
    """

    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2, 3],
            "event": ["ADD", "ADD", "TRADE"],
            "order_id": [1, 2, 1],
            "side": ["B", "B", "B"],
            "price": [10.0, 10.0, 10.0],
            "volume": [100, 50, 100],
        },
    )

    out = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
        )
    )

    snapshot = pq.read_table(out)

    # -----------------------------
    # 断言盘口
    # -----------------------------

    assert snapshot.num_rows == 1

    row = snapshot.to_pylist()[0]

    # 剩余的应是第二个订单
    assert row["side"] == "B"
    assert row["price"] == 10.0
    assert row["volume"] == 50
    assert row["level"] == 1

def test_fifo_partial_then_full_does_not_touch_next(tmp_path, make_events_parquet):
    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2, 3, 4],
            "event": ["ADD", "ADD", "TRADE", "TRADE"],
            "order_id": [1, 2, 1, 1],
            "side": ["S", "S", "S", "S"],
            "price": [20.0, 20.0, 20.0, 20.0],
            "volume": [100, 50, 60, 40],
        },
    )

    out = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
        )
    )

    snapshot = pq.read_table(out)

    assert snapshot.num_rows == 1

    row = snapshot.to_pylist()[0]

    assert row["side"] == "S"
    assert row["price"] == 20.0
    assert row["volume"] == 50

def test_multi_price_level_sorting(tmp_path, make_events_parquet):
    """
    多价位排序正确性：
        - B: 高 → 低
        - S: 低 → 高
    """

    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2, 3, 4],
            "event": ["ADD", "ADD", "ADD", "ADD"],
            "order_id": [1, 2, 3, 4],
            "side": ["B", "B", "S", "S"],
            "price": [10.0, 12.0, 11.0, 9.5],
            "volume": [100, 200, 150, 120],
        },
    )

    out = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
        )
    )

    snapshot = pq.read_table(out).to_pylist()

    # -----------------------------
    # 拆分买卖盘
    # -----------------------------
    bids = [r for r in snapshot if r["side"] == "B"]
    asks = [r for r in snapshot if r["side"] == "S"]

    # -----------------------------
    # 买盘：高 → 低
    # -----------------------------
    assert [r["price"] for r in bids] == [12.0, 10.0]
    assert [r["level"] for r in bids] == [1, 2]

    # -----------------------------
    # 卖盘：低 → 高
    # -----------------------------
    assert [r["price"] for r in asks] == [9.5, 11.0]
    assert [r["level"] for r in asks] == [1, 2]
