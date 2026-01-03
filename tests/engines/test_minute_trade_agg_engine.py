from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from src.data_system.engines.minute_trade_agg_engine import MinuteTradeAggEngine


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def us(dt: datetime) -> int:
    """datetime -> epoch microseconds"""
    return int(dt.timestamp() * 1_000_000)


def table_from_rows(rows: list[dict]) -> pa.Table:
    return pa.table({k: [r[k] for r in rows] for k in rows[0]})


# -----------------------------------------------------------------------------
# 1. timezone / minute definition (最关键防回归测试)
# -----------------------------------------------------------------------------
def test_minute_trade_agg_respects_exchange_timezone():
    """
    冻结契约：

    - ts 是 absolute instant（epoch us）
    - minute_local_us 表示【交易所本地钟面 minute 起点】
    - 不允许隐式 UTC minute
    - 不使用 datetime / tz 进行下游解释

    TickTime（Asia/Shanghai）:
        09:25:01 CST == 01:25:01 UTC

    期望：
        minute_local_us == 2025-12-01 09:25:00（Shanghai wall-clock）
        即 absolute instant == 2025-12-01 01:25:00 UTC
    """

    engine = MinuteTradeAggEngine(exchange="CN")

    # 2025-12-01 09:25:01 Asia/Shanghai
    # == 2025-12-01 01:25:01 UTC
    ts = us(datetime(2025, 12, 1, 1, 25, 1, tzinfo=timezone.utc))

    table = pa.table(
        {
            "ts": [ts],
            "price": [10.0],
            "volume": [100],
            "notional": [1000.0],
            "symbol": ["600000"],
        }
    )

    out = engine.execute(table)
    assert out.num_rows == 1

    # --------------------------------------------------
    # 核心断言 1：absolute instant（唯一真相）
    # --------------------------------------------------
    minute_local_us = out["ts"][0].as_py()

    # 上海本地钟面 09:25:00，对应的 epoch us（本地时间轴）
    expected_minute_local_us = us(
        datetime(2025, 12, 1, 9, 25, 0, tzinfo=timezone.utc)
    )

    assert minute_local_us == expected_minute_local_us

    # 冗余一致性校验
    assert out["minute"][0].as_py() == 925
    assert out["minute_str"][0].as_py() == "09:25"
    assert out["trade_date"][0].as_py() == 20251201


# -----------------------------------------------------------------------------
# 2. 基本 OHLCV 语义
# -----------------------------------------------------------------------------
def test_minute_trade_agg_single_minute_basic():
    engine = MinuteTradeAggEngine()

    base = datetime(2025, 1, 1, 9, 30, tzinfo=timezone.utc)

    table = pa.table(
        {
            "ts": [
                us(base + timedelta(seconds=1)),
                us(base + timedelta(seconds=10)),
                us(base + timedelta(seconds=30)),
            ],
            "price": [10.0, 10.5, 10.2],
            "volume": [100, 50, 20],
            "notional": [1000.0, 525.0, 204.0],
            "symbol": ["600000"] * 3,
        }
    )

    out = engine.execute(table)
    assert out.num_rows == 1

    d = out.to_pydict()
    assert d["open"] == [10.0]
    assert d["high"] == [10.5]
    assert d["low"] == [10.0]
    assert d["close"] == [10.2]
    assert d["volume"] == [170]
    assert d["notional"] == [1729.0]
    assert d["trade_count"] == [3]


def test_minute_trade_agg_multiple_minutes():
    engine = MinuteTradeAggEngine()

    t0 = datetime(2025, 1, 1, 9, 30, tzinfo=timezone.utc)

    table = pa.table(
        {
            "ts": [
                us(t0 + timedelta(seconds=1)),
                us(t0 + timedelta(seconds=10)),
                us(t0 + timedelta(minutes=1, seconds=5)),
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


# -----------------------------------------------------------------------------
# 3. 跨日 / gap 语义
# -----------------------------------------------------------------------------
def test_minute_trade_agg_cross_day_not_merged():
    """
    23:59 → 09:30 必须是两个 distinct minute
    """
    engine = MinuteTradeAggEngine()

    table = pa.table(
        {
            "ts": [
                us(datetime(2025, 1, 1, 23, 59, 10, tzinfo=timezone.utc)),
                us(datetime(2025, 1, 2, 9, 30, 5, tzinfo=timezone.utc)),
            ],
            "price": [10.0, 10.5],
            "volume": [100, 50],
            "notional": [1000.0, 525.0],
            "symbol": ["600000", "600000"],
        }
    )

    out = engine.execute(table)
    assert out.num_rows == 2


def test_minute_trade_agg_no_fill_on_cross_day_gap():
    """
    不补中间缺失 minute
    """
    engine = MinuteTradeAggEngine()

    table = pa.table(
        {
            "ts": [
                us(datetime(2025, 1, 1, 15, 0, tzinfo=timezone.utc)),
                us(datetime(2025, 1, 2, 9, 30, tzinfo=timezone.utc)),
            ],
            "price": [10.0, 10.2],
            "volume": [100, 50],
            "notional": [1000.0, 510.0],
            "symbol": ["600000", "600000"],
        }
    )

    out = engine.execute(table)
    assert out.num_rows == 2


# -----------------------------------------------------------------------------
# 4. 输入前提约束
# -----------------------------------------------------------------------------
def test_minute_trade_agg_rejects_unsorted_ts():
    """
    输入必须按 ts 升序
    """
    engine = MinuteTradeAggEngine()

    table = pa.table(
        {
            "ts": [
                us(datetime(2025, 1, 1, 9, 30, 10, tzinfo=timezone.utc)),
                us(datetime(2025, 1, 1, 9, 30, 1, tzinfo=timezone.utc)),
            ],
            "price": [10.2, 10.0],
            "volume": [10, 10],
            "notional": [102.0, 100.0],
            "symbol": ["600000", "600000"],
        }
    )

    with pytest.raises(ValueError, match="requires input sorted by ts"):
        engine.execute(table)


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


# -----------------------------------------------------------------------------
# 5. symbol 隔离语义
# -----------------------------------------------------------------------------
def test_minute_trade_agg_symbol_isolation():
    engine = MinuteTradeAggEngine()

    base = datetime(2025, 1, 1, 9, 30, tzinfo=timezone.utc)

    table = pa.table(
        {
            "ts": [
                us(base + timedelta(seconds=1)),
                us(base + timedelta(seconds=10)),
                us(base + timedelta(seconds=5)),
                us(base + timedelta(seconds=20)),
            ],
            "symbol": ["600000", "600000", "000001", "000001"],
            "price": [10.0, 10.5, 20.0, 19.5],
            "volume": [100, 50, 200, 100],
            "notional": [1000.0, 525.0, 4000.0, 1950.0],
        }
    )

    a = table.filter(pc.equal(table["symbol"], "600000"))
    b = table.filter(pc.equal(table["symbol"], "000001"))

    out_a = engine.execute(a)
    out_b = engine.execute(b)

    da = out_a.to_pydict()
    db = out_b.to_pydict()

    assert da["open"] == [10.0]
    assert da["close"] == [10.5]
    assert da["volume"] == [150]

    assert db["open"] == [20.0]
    assert db["close"] == [19.5]
    assert db["volume"] == [300]


def test_minute_trade_agg_minute_boundary_precision():
    """
    冻结语义：
      - 09:25:59.999 → 09:25
      - 09:26:00.000 → 09:26
    """
    engine = MinuteTradeAggEngine(exchange="CN")

    base = datetime(2025, 12, 1, 1, 25, 0, tzinfo=timezone.utc)

    table = pa.table(
        {
            "ts": [
                us(base + timedelta(seconds=59, microseconds=999_000)),
                us(base + timedelta(minutes=1)),
            ],
            "price": [10.0, 10.1],
            "volume": [1, 1],
            "notional": [10.0, 10.1],
            "symbol": ["600000", "600000"],
        }
    )

    out = engine.execute(table)
    d = out.to_pydict()

    assert d["minute"] == [925, 926]
    assert d["minute_str"] == ["09:25", "09:26"]
def test_minute_trade_agg_local_midnight_rollover():
    """
    冻结语义：
      本地 23:59 → 次日 00:00
      必须：
        - minute 变化
        - trade_date +1
    """
    engine = MinuteTradeAggEngine(exchange="CN")

    # 本地时间：
    # 2025-12-01 23:59:30 CST == 15:59:30 UTC
    # 2025-12-02 00:00:10 CST == 16:00:10 UTC
    table = pa.table(
        {
            "ts": [
                us(datetime(2025, 12, 1, 15, 59, 30, tzinfo=timezone.utc)),
                us(datetime(2025, 12, 1, 16, 0, 10, tzinfo=timezone.utc)),
            ],
            "price": [10.0, 10.2],
            "volume": [1, 1],
            "notional": [10.0, 10.2],
            "symbol": ["600000", "600000"],
        }
    )

    out = engine.execute(table)
    d = out.to_pydict()

    assert d["trade_date"] == [20251201, 20251202]
    assert d["minute"] == [2359, 0]
    assert d["minute_str"] == ["23:59", "00:00"]
def test_minute_trade_agg_time_fields_consistency():
    """
    冻结不变量：
      minute_local_us
      trade_date
      minute
      minute_str
    必须来自同一个“本地钟面时间”
    """
    from src.data_system.engines.minute_trade_agg_engine import US_PER_DAY,US_PER_MINUTE, US_PER_HOUR
    engine = MinuteTradeAggEngine(exchange="CN")

    ts = us(datetime(2025, 6, 18, 2, 15, 45, tzinfo=timezone.utc))  # 10:15:45 CST

    table = pa.table(
        {
            "ts": [ts],
            "price": [10.0],
            "volume": [1],
            "notional": [10.0],
            "symbol": ["600000"],
        }
    )

    out = engine.execute(table)
    row = out.to_pydict()

    assert row["trade_date"] == [20250618]
    assert row["minute"] == [1015]
    assert row["minute_str"] == ["10:15"]

    # 再验证 minute_local_us 反推分钟
    minute_local_us = row["ts"][0]
    minute_mod_day = minute_local_us % US_PER_DAY

    assert minute_mod_day // US_PER_HOUR == 10
    assert (minute_mod_day % US_PER_HOUR) // US_PER_MINUTE == 15
def test_minute_trade_agg_same_minute_different_symbols_isolated():
    engine = MinuteTradeAggEngine(exchange="CN")

    base = datetime(2025, 1, 1, 9, 30, tzinfo=timezone.utc)

    table = pa.table(
        {
            "ts": [
                us(base),
                us(base),
            ],
            "symbol": ["600000", "000001"],
            "price": [10.0, 20.0],
            "volume": [100, 200],
            "notional": [1000.0, 4000.0],
        }
    )

    a = table.filter(pc.equal(table["symbol"], "600000"))
    b = table.filter(pc.equal(table["symbol"], "000001"))

    out_a = engine.execute(a)
    out_b = engine.execute(b)

    assert out_a["open"].to_pylist() == [10.0]
    assert out_b["open"].to_pylist() == [20.0]
