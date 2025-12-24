import pyarrow as pa
from datetime import datetime, timezone

from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine, US_PER_MINUTE


def _us(ts: datetime) -> int:
    return int(ts.timestamp() * 1_000_000)


def test_minute_trade_agg_cross_day_not_merged():
    """
    冻结契约：

    MinuteTradeAggEngine 只按时间戳聚合，
    不做任何交易日 / 跨日推断。

    23:59 → 09:30 必须产生两个 distinct minute。
    """

    engine = MinuteTradeAggEngine()

    table = pa.table(
        {
            "ts": [
                _us(datetime(2025, 1, 1, 23, 59, 10, tzinfo=timezone.utc)),
                _us(datetime(2025, 1, 2, 9, 30, 5, tzinfo=timezone.utc)),
            ],
            "price": [10.0, 10.5],
            "volume": [100, 50],
            "notional": [1000.0, 525.0],
            "symbol": ["600000", "600000"],
        }
    )

    out = engine.execute(table)

    assert out.num_rows == 2
def test_minute_trade_agg_cross_day_minute_ts_correct():
    """
    冻结不变量：

    输出 minute = floor(ts / 60s) * 60s
    与是否跨日无关。
    """

    engine = MinuteTradeAggEngine()

    t1 = datetime(2025, 1, 1, 23, 59, 59, tzinfo=timezone.utc)
    t2 = datetime(2025, 1, 2, 9, 30, 1, tzinfo=timezone.utc)

    table = pa.table(
        {
            "ts": [_us(t1), _us(t2)],
            "price": [10.0, 10.5],
            "volume": [1, 1],
            "notional": [10.0, 10.5],
            "symbol": ["600000", "600000"],
        }
    )

    out = engine.execute(table)
    minutes = out["minute"].to_pylist()

    assert minutes[0] == datetime(2025, 1, 1, 23, 59)
    assert minutes[1] == datetime(2025, 1, 2, 9, 30)
def test_minute_trade_agg_no_fill_on_cross_day_gap():
    """
    冻结契约：

    MinuteTradeAggEngine 不补缺失 minute。
    跨日 / 休市 gap 必须保持稀疏。
    """

    engine = MinuteTradeAggEngine()

    table = pa.table(
        {
            "ts": [
                _us(datetime(2025, 1, 1, 15, 0, 0, tzinfo=timezone.utc)),
                _us(datetime(2025, 1, 2, 9, 30, 0, tzinfo=timezone.utc)),
            ],
            "price": [10.0, 10.2],
            "volume": [100, 50],
            "notional": [1000.0, 510.0],
            "symbol": ["600000", "600000"],
        }
    )

    out = engine.execute(table)

    # 只有两根 bar，不补中间的几百分钟
    assert out.num_rows == 2
