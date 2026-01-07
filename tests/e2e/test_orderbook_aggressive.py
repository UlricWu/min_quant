import pyarrow as pa
from src.data_system.engines.trade_enrich_engine import TradeEnrichEngine
from src.data_system.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.data_system.engines.context import EngineContext

import pyarrow.compute as pc
def make_canonical_events(rows: list[dict]) -> pa.Table:
    return pa.table(
        {
            "ts":        [r["ts"] for r in rows],
            "event":     [r["event"] for r in rows],
            "order_id":  [r["order_id"] for r in rows],
            "side":      [r.get("side") for r in rows],
            "price":     [r.get("price") for r in rows],
            "volume":    [r.get("volume") for r in rows],
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



def test_aggressive_trade_direction():
    """
    主动吃单：
      - 高于前价成交 → 主动买 (+1)
      - 低于前价成交 → 主动卖 (-1)
    """

    table = make_canonical_events([
        dict(ts=1, event="ADD",   order_id=1, side="S", price=10.2, volume=100),
        dict(ts=2, event="ADD",   order_id=2, side="B", price=10.0, volume=100),

        dict(ts=3, event="TRADE", order_id=2, side=None, price=10.3, volume=50),  # 吃卖盘
        dict(ts=4, event="TRADE", order_id=1, side=None, price=9.9,  volume=50),  # 吃买盘
    ])

    enriched = TradeEnrichEngine().execute(table)

    trade_sides = [
        s for s, e in zip(
            enriched["trade_side"].to_pylist(),
            enriched["event"].to_pylist()
        ) if e == "TRADE"
    ]

    assert trade_sides == [1, -1]
def test_passive_trade_direction():
    """
    被动成交：
      - 成交价 == 前一成交价
      - tick rule → trade_side = 0
    """

    table = make_canonical_events([
        dict(ts=1, event="ADD",   order_id=1, side="S", price=10.1, volume=100),
        dict(ts=2, event="TRADE", order_id=1, side=None, price=10.1, volume=20),
        dict(ts=3, event="TRADE", order_id=1, side=None, price=10.1, volume=30),
    ])

    enriched = TradeEnrichEngine().execute(table)

    trade_sides = [
        s for s, e in zip(
            enriched["trade_side"].to_pylist(),
            enriched["event"].to_pylist()
        ) if e == "TRADE"
    ]

    assert trade_sides == [0, 0]
def test_orderbook_side_reversal_pressure():
    """
    盘口反转压力测试：

    - 第一笔 TRADE：主动买（吃卖盘）
    - 第二笔 TRADE：主动卖（吃买盘）
    """

    table = make_canonical_events([
        dict(ts=1, event="ADD",   order_id=1, side="S", price=10.2, volume=100),
        dict(ts=2, event="ADD",   order_id=2, side="B", price=10.0, volume=100),

        dict(ts=3, event="TRADE", order_id=2, side=None, price=10.3, volume=40),
        dict(ts=4, event="TRADE", order_id=1, side=None, price=9.8,  volume=40),
    ])

    enriched = TradeEnrichEngine().execute(table)

    trade_sides = [
        s for s, e in zip(
            enriched["trade_side"].to_pylist(),
            enriched["event"].to_pylist()
        ) if e == "TRADE"
    ]

    assert trade_sides == [1, -1]
def test_trade_side_matches_orderbook_consumption(tmp_path):
    """
    不变量（冻结）：
      - trade_side == +1 → ask_qty 减少
      - trade_side == -1 → bid_qty 减少
    """

    table = make_canonical_events([
        dict(ts=1, event="ADD",   order_id=1, side="S", price=10.2, volume=100),
        dict(ts=2, event="ADD",   order_id=2, side="B", price=10.0, volume=100),
        dict(ts=3, event="TRADE", order_id=2, side=None, price=10.3, volume=30),
    ])

    enriched = TradeEnrichEngine().execute(table)


    # 写 parquet 供 OrderBook 使用
    input_path = tmp_path / "events.parquet"
    pa.parquet.write_table(enriched, input_path)

    out_path = tmp_path / "snapshot.parquet"

    engine = OrderBookRebuildEngine()

    ctx = EngineContext(
        mode="offline",
        input_file=input_path,
        output_file=out_path,
    )

    engine.execute(ctx)

    snapshot = pa.parquet.read_table(out_path)

    ask_rows = snapshot.filter(
        pa.compute.equal(snapshot["side"], "S")
    )

    bid_rows = snapshot.filter(pc.equal(snapshot["side"], "B"))
    ask_rows = snapshot.filter(pc.equal(snapshot["side"], "S"))

    # TRADE 发生在买单上 → 买盘减少
    assert any(v < 100 for v in bid_rows["volume"].to_pylist())

    # 卖盘不受影响
    assert all(v == 100 for v in ask_rows["volume"].to_pylist())

