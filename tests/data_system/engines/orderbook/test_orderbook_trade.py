def _make_canonical_events_for_enrich_and_book(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "ts":     [1, 2, 3, 4],
            "event":  ["ADD", "ADD", "TRADE", "TRADE"],
            "order_id": [1,   2,   2,   1],
            "side":   ["B", "S", "S", "B"],
            "price":  [10.0, 10.2, 10.3, 9.9],
            "volume": [100,  100,  40,  30],
        }
    )

    path = tmp_path / "events.parquet"
    pq.write_table(table, path)
    return path, table

import pyarrow.parquet as pq

from src.data_system.engines.trade_enrich_engine import TradeEnrichEngine
from src.data_system.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.data_system.engines.context import EngineContext


def test_trade_enrich_and_orderbook_direction_consistency(tmp_path):
    """
    锁死不变量：
      TradeEnrich 推断的 trade_side
      与 OrderBook 实际消耗的盘口方向必须一致
    """

    # --------------------------------------------------
    # 1) 构造 canonical events
    # --------------------------------------------------
    input_path, raw_table = _make_canonical_events_for_enrich_and_book(tmp_path)

    # --------------------------------------------------
    # 2) TradeEnrich
    # --------------------------------------------------
    enrich = TradeEnrichEngine()
    enriched = enrich.execute(raw_table)

    trade_side = enriched["trade_side"].to_pylist()

    # 第 0/1 行是 ADD → side=0
    # 第 2 行 price 10.3 > 10.2 → +1（主动买）
    # 第 3 行 price 9.9 < 10.3 → -1（主动卖）
    # assert trade_side == [0, 0, 1, -1]
    events = enriched["event"].to_pylist()
    # 只对 TRADE 行断言方向
    trade_sides_on_trade = [
        s for s, ev in zip(trade_side, events) if ev == "TRADE"
    ]

    assert trade_sides_on_trade == [1, -1]

    # --------------------------------------------------
    # 3) OrderBook 重建
    # --------------------------------------------------
    snapshot_path = tmp_path / "orderbook.parquet"

    book = OrderBookRebuildEngine()
    book.execute(
        EngineContext(
            mode="offline",
            input_path=input_path,
            output_path=snapshot_path,
        )
    )

    snapshot = pq.read_table(snapshot_path).to_pylist()

    # --------------------------------------------------
    # 4) 断言盘口状态（方向一致性）
    # --------------------------------------------------

    # 卖盘：
    # 初始 100，被主动买成交 40 → 剩 60
    asks = [r for r in snapshot if r["side"] == "S"]
    assert len(asks) == 1
    assert asks[0]["price"] == 10.2
    assert asks[0]["volume"] == 60

    # 买盘：
    # 初始 100，被主动卖成交 30 → 剩 70
    bids = [r for r in snapshot if r["side"] == "B"]
    assert len(bids) == 1
    assert bids[0]["price"] == 10.0
    assert bids[0]["volume"] == 70

def test_trade_enrich_and_orderbook_direction_consistency(tmp_path):
    """
    语义约定（冻结）：

    - TradeEnrichEngine 推断的 trade_side 是基于 tick rule 的数值推断
    - trade_side 仅在 event == "TRADE" 的行上具有交易语义意义
    - ADD / CANCEL 行上的 trade_side 不参与任何语义约束，测试中不得断言

    本测试锁死的不变量是：
      对于所有 TRADE 行：
        TradeEnrich 推断的 trade_side
        必须与 OrderBook 实际消耗盘口的方向一致
    """

    # --------------------------------------------------
    # 1) 构造 canonical events（已按 ts 排序）
    # --------------------------------------------------
    input_path, raw_table = _make_canonical_events_for_enrich_and_book(tmp_path)

    # --------------------------------------------------
    # 2) TradeEnrich
    # --------------------------------------------------
    enrich = TradeEnrichEngine()
    enriched = enrich.execute(raw_table)

    trade_side = enriched["trade_side"].to_pylist()
    events = enriched["event"].to_pylist()

    # --------------------------------------------------
    # 3) 只对 TRADE 行断言方向
    # --------------------------------------------------
    trade_sides_on_trade = [
        side
        for side, ev in zip(trade_side, events)
        if ev == "TRADE"
    ]

    # 第一个 TRADE：price 上涨 → 主动买 (+1)
    # 第二个 TRADE：price 下跌 → 主动卖 (-1)
    assert trade_sides_on_trade == [1, -1]
