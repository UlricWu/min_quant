import pyarrow as pa
from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine, US_PER_MINUTE


def test_minute_trade_agg_single_minute_basic():
    engine = MinuteTradeAggEngine()

    base = 10 * US_PER_MINUTE
    table = pa.table(
        {
            "ts": [
                base + 1,
                base + 10,
                base + 30,
            ],
            "price": [10.0, 10.5, 10.2],
            "volume": [100, 50, 20],
            "notional": [1000.0, 525.0, 204.0],
            "symbol": ["600000"] * 3,
        }
    )

    out = engine.execute(table)

    assert out.num_rows == 1

    row = out.to_pydict()

    assert row["open"] == [10.0]
    assert row["high"] == [10.5]
    assert row["low"] == [10.0]
    assert row["close"] == [10.2]

    assert row["volume"] == [170]
    assert row["notional"] == [1729.0]
    assert row["trade_count"] == [3]

def test_minute_trade_agg_multiple_minutes():
    engine = MinuteTradeAggEngine()

    table = pa.table(
        {
            "ts": [
                1 * US_PER_MINUTE + 1,
                1 * US_PER_MINUTE + 10,
                2 * US_PER_MINUTE + 5,
            ],
            "price": [10.0, 10.2, 10.1],
            "volume": [100, 50, 30],
            "notional": [1000.0, 510.0, 303.0],
            "symbol": ["600000"] * 3,
        }
    )

    out = engine.execute(table)

    assert out.num_rows == 2

    d = out.to_pydict()

    assert d["open"] == [10.0, 10.1]
    assert d["close"] == [10.2, 10.1]
    assert d["trade_count"] == [2, 1]
def test_minute_trade_agg_empty_input():
    engine = MinuteTradeAggEngine()

    table = pa.table(
        {
            "ts": [],
            "price": [],
            "volume": [],
            "notional": [],
            "symbol": [],
        }
    )

    out = engine.execute(table)

    assert out.num_rows == 0

