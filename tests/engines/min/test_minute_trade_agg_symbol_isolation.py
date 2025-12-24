import pyarrow as pa
from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine, US_PER_MINUTE


def test_minute_trade_agg_symbol_isolation_basic():
    """
    冻结语义：
      - MinuteTradeAggEngine 假设输入为单 symbol
      - 多 symbol 场景必须通过 SymbolAccessor 拆分
      - 各 symbol 的 minute 聚合结果必须完全独立
    """

    engine = MinuteTradeAggEngine()
    base = 10 * US_PER_MINUTE

    # --------------------------------------------------
    # 构造两个 symbol 的 canonical enriched trades
    # --------------------------------------------------
    table = pa.table(
        {
            "ts": [
                base + 1, base + 10,      # symbol A
                base + 5, base + 20,      # symbol B
            ],
            "symbol": [
                "600000", "600000",
                "000001", "000001",
            ],
            "price": [
                10.0, 10.5,               # A
                20.0, 19.5,               # B
            ],
            "volume": [
                100, 50,
                200, 100,
            ],
            "notional": [
                1000.0, 525.0,
                4000.0, 1950.0,
            ],
        }
    )

    # --------------------------------------------------
    # 模拟 SymbolAccessor 行为：按 symbol 拆分
    # --------------------------------------------------
    table_a = table.filter(pa.compute.equal(table["symbol"], "600000"))
    table_b = table.filter(pa.compute.equal(table["symbol"], "000001"))

    out_a = engine.execute(table_a)
    out_b = engine.execute(table_b)

    # --------------------------------------------------
    # 验证 symbol A
    # --------------------------------------------------
    da = out_a.to_pydict()

    assert da["open"] == [10.0]
    assert da["close"] == [10.5]
    assert da["high"] == [10.5]
    assert da["low"] == [10.0]
    assert da["volume"] == [150]
    assert da["notional"] == [1525.0]
    assert da["trade_count"] == [2]

    # --------------------------------------------------
    # 验证 symbol B
    # --------------------------------------------------
    db = out_b.to_pydict()

    assert db["open"] == [20.0]
    assert db["close"] == [19.5]
    assert db["high"] == [20.0]
    assert db["low"] == [19.5]
    assert db["volume"] == [300]
    assert db["notional"] == [5950.0]
    assert db["trade_count"] == [2]

def test_minute_trade_agg_same_minute_different_symbol_isolated():
    """
    冻结不变量：
      即使两个 symbol 的成交发生在完全相同的 minute，
      其聚合结果也绝不允许互相影响。
    """

    engine = MinuteTradeAggEngine()
    base = 42 * US_PER_MINUTE

    table = pa.table(
        {
            "ts": [
                base + 1,
                base + 2,
            ],
            "symbol": [
                "600000",
                "000001",
            ],
            "price": [
                10.0,
                20.0,
            ],
            "volume": [
                100,
                200,
            ],
            "notional": [
                1000.0,
                4000.0,
            ],
        }
    )

    a = table.filter(pa.compute.equal(table["symbol"], "600000"))
    b = table.filter(pa.compute.equal(table["symbol"], "000001"))

    out_a = engine.execute(a)
    out_b = engine.execute(b)

    assert out_a["open"].to_pylist() == [10.0]
    assert out_b["open"].to_pylist() == [20.0]

    assert out_a["volume"].to_pylist() == [100]
    assert out_b["volume"].to_pylist() == [200]

def test_minute_trade_agg_empty_symbol_does_not_affect_others():
    """
    冻结不变量：
      某个 symbol 无成交（空表），
      不得影响其他 symbol 的 minute 聚合结果。
    """

    engine = MinuteTradeAggEngine()
    base = 7 * US_PER_MINUTE

    table = pa.table(
        {
            "ts": [base + 1],
            "symbol": ["600000"],
            "price": [10.0],
            "volume": [100],
            "notional": [1000.0],
        }
    )

    a = table.filter(pa.compute.equal(table["symbol"], "600000"))
    b = table.filter(pa.compute.equal(table["symbol"], "000001"))  # empty

    out_a = engine.execute(a)
    out_b = engine.execute(b)

    assert out_a.num_rows == 1
    assert out_b.num_rows == 0
