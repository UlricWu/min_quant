from src.data_system.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.data_system.engines.context import EngineContext

import pyarrow.parquet as pq


def test_record_events_consistent_with_snapshot(tmp_path, make_events_parquet):
    """
    record_events=True 时：
        - orderbook_events.parquet 必须存在
        - event 流与 snapshot 在语义上完全一致
    """

    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2, 3, 4],
            "event": ["ADD", "ADD", "TRADE", "CANCEL"],
            "order_id": [1, 2, 1, 2],
            "side": ["B", "B", "B", "B"],
            "price": [10.0, 11.0, 10.0, 11.0],
            "volume": [100, 50, 40, 0],
        },
    )

    snapshot_path = tmp_path / "orderbook.parquet"
    events_path = tmp_path / "orderbook_events.parquet"

    engine = OrderBookRebuildEngine(record_events=True)
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=snapshot_path,
        )
    )

    # ======================================================
    # 1️⃣ events 文件必须存在
    # ======================================================
    assert events_path.exists()

    events = pq.read_table(events_path)
    snapshot = pq.read_table(snapshot_path)

    # ======================================================
    # 2️⃣ 事件数量一致（没有多记 / 漏记）
    # ======================================================
    assert events.num_rows == 4

    # ======================================================
    # 3️⃣ 事件顺序必须保持
    # ======================================================
    assert events.column("event").to_pylist() == [
        "ADD",
        "ADD",
        "TRADE",
        "CANCEL",
    ]

    # ======================================================
    # 4️⃣ snapshot 只剩 1 档（order_id=1）
    # ======================================================
    rows = snapshot.to_pylist()
    assert len(rows) == 1

    row = rows[0]

    assert row["side"] == "B"
    assert row["price"] == 10.0
    assert row["volume"] == 60  # 100 - 40
    assert row["level"] == 1
