from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
"""
ADD → TRADE
ADD → TRADE
连续部分成交
重复 ADD
部分成交 → 再部分成交 → 再成交完

TRADE 完全成交（volume == 剩余量）

TRADE 超额（volume > 剩余量）防御
ADD → CANCEL → TRADE（非法序列）防御测试

"""

from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.pipeline.context import EngineContext
import pytest
def _make_parquet(path: Path) -> Path:
    table = pa.table(
        {
            "ts": [1, 2, 3],
            "event": ["ADD", "TRADE", "ADD"],
            "order_id": [1, 1, 2],
            "side": ["B", "B", "S"],
            "price": [10.0, 10.0, 10.5],
            "volume": [100, 40, 200],
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

    pq.write_table(table, path)
    return path



def test_trade_partial_fill_keeps_order(tmp_path: Path):
    input_path = _make_parquet(tmp_path / "events.parquet")
    output_path = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine(record_events=False)

    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=output_path,
            key=''
        )
    )

    snapshot = pq.read_table(output_path).to_pydict()

    # -------------------------
    # 校验盘口条数
    # -------------------------
    assert len(snapshot["side"]) == 2

    # -------------------------
    # BID：部分成交后仍存在
    # -------------------------
    assert snapshot["side"][0] == "B"
    assert snapshot["level"][0] == 1
    assert snapshot["price"][0] == 10.0
    assert snapshot["volume"][0] == 60  # 100 - 40

    # -------------------------
    # ASK：完整保留
    # -------------------------
    assert snapshot["side"][1] == "S"
    assert snapshot["level"][1] == 1
    assert snapshot["price"][1] == 10.5
    assert snapshot["volume"][1] == 200

    # ts = 最后事件
    assert snapshot["ts"] == [3, 3]



def test_trade_partial_partial_then_filled(tmp_path, make_events_parquet):
    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2, 3, 4],
            "event": ["ADD", "TRADE", "TRADE", "TRADE"],
            "order_id": [1, 1, 1, 1],
            "side": ["B", "B", "B", "B"],
            "price": [10.0, 10.0, 10.0, 10.0],
            "volume": [100, 30, 20, 50],
        },
    )

    out = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
            key=''
        )
    )

    snapshot = pq.read_table(out)

    # 完全成交后，盘口应为空
    assert snapshot.num_rows == 0

def test_trade_exact_fill_removes_order(tmp_path, make_events_parquet):
    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2],
            "event": ["ADD", "TRADE"],
            "order_id": [1, 1],
            "side": ["B", "B"],
            "price": [10.0, 10.0],
            "volume": [100, 100],
        },
    )

    out = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
            key=''
        )
    )

    snapshot = pq.read_table(out)

    assert snapshot.num_rows == 0
def test_trade_exact_fill_removes_order(tmp_path, make_events_parquet):
    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2],
            "event": ["ADD", "TRADE"],
            "order_id": [1, 1],
            "side": ["B", "B"],
            "price": [10.0, 10.0],
            "volume": [100, 100],
        },
    )

    out = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
            key=''
        )
    )

    snapshot = pq.read_table(out)

    assert snapshot.num_rows == 0
def test_trade_overfill_is_defensive(tmp_path, make_events_parquet):
    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2],
            "event": ["ADD", "TRADE"],
            "order_id": [1, 1],
            "side": ["S", "S"],
            "price": [20.0, 20.0],
            "volume": [50, 80],  # 超额成交
        },
    )

    out = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
            key=''
        )
    )

    snapshot = pq.read_table(out)

    # 不应出现负 volume / 残留订单
    assert snapshot.num_rows == 0
def test_add_cancel_then_trade_is_ignored(tmp_path, make_events_parquet):
    """
    非法但现实存在的序列：
        ADD → CANCEL → TRADE

    期望行为：
        - 不抛异常
        - TRADE 被静默忽略
        - OrderBook 最终为空
    """

    input_path = make_events_parquet(
        tmp_path / "events.parquet",
        {
            "ts": [1, 2, 3],
            "event": ["ADD", "CANCEL", "TRADE"],
            "order_id": [1001, 1001, 1001],
            "side": ["B", "B", "B"],
            "price": [10.5, 10.5, 10.5],
            "volume": [100, 0, 50],  # TRADE volume 任意
        },
    )

    out = tmp_path / "orderbook.parquet"

    engine = OrderBookRebuildEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_file=input_path,
            output_file=out,
            key=''
        )
    )

    snapshot = pq.read_table(out)

    # -----------------------------
    # 关键断言
    # -----------------------------

    # 1️⃣ OrderBook 不应 resurrect 已撤销订单
    assert snapshot.num_rows == 0





