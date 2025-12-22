import pandas as pd
import pytest

from logs.engines.aggregate_minute_bar_engine import AggregateMinuteBarEngine
@pytest.fixture
def engine():
    return AggregateMinuteBarEngine()


@pytest.fixture
def simple_trade_df():
    """
    ts 单位：ms
    覆盖两个分钟
    """
    return pd.DataFrame(
        {
            "ts": [
                60_000 + 1_000,   # 01:01
                60_000 + 5_000,   # 01:05
                120_000 + 2_000,  # 02:02
            ],
            "price": [10.0, 12.0, 11.0],
            "volume": [100, 200, 300],
        }
    )
def test_missing_required_columns(engine):
    df = pd.DataFrame({"ts": [0], "price": [1.0]})

    with pytest.raises(ValueError, match="Missing required columns"):
        engine.run(df)
def test_ts_must_be_int(engine):
    df = pd.DataFrame(
        {
            "ts": [1.0, 2.0],
            "price": [10.0, 11.0],
            "volume": [1, 1],
        }
    )

    with pytest.raises(TypeError, match="ts"):
        engine.run(df)
@pytest.mark.parametrize(
    "price, volume",
    [
        (0.0, 10),
        (-1.0, 10),
        (10.0, 0),
        (10.0, -5),
    ],
)
def test_price_volume_constraints(engine, price, volume):
    df = pd.DataFrame(
        {
            "ts": [0],
            "price": [price],
            "volume": [volume],
        }
    )

    with pytest.raises(ValueError):
        engine.run(df)
def test_trade_df_must_be_sorted(engine):
    df = pd.DataFrame(
        {
            "ts": [2_000, 1_000],
            "price": [10.0, 11.0],
            "volume": [1, 1],
        }
    )

    with pytest.raises(ValueError, match="sorted"):
        engine.run(df)
def test_minute_alignment(engine):
    df = pd.DataFrame(
        {
            "ts": [
                60_000 + 1,     # 01:00
                60_000 + 59_999,
                120_000 + 5,    # 02:00
            ],
            "price": [10.0, 11.0, 12.0],
            "volume": [1, 1, 1],
        }
    )

    out = engine.run(df)

    assert out["ts"].tolist() == [
        60_000,
        120_000,
    ]
def test_aggregation_semantics(engine, simple_trade_df):
    out = engine.run(simple_trade_df)

    # 第一根 bar（minute = 60_000）
    first = out.iloc[0]

    assert first["open"] == 10.0
    assert first["high"] == 12.0
    assert first["low"] == 10.0
    assert first["close"] == 12.0
    assert first["volume"] == 300
    assert first["trade_count"] == 2
    assert first["turnover"] == 10.0 * 100 + 12.0 * 200

    # 第二根 bar
    second = out.iloc[1]

    assert second["open"] == 11.0
    assert second["high"] == 11.0
    assert second["low"] == 11.0
    assert second["close"] == 11.0
    assert second["volume"] == 300
    assert second["trade_count"] == 1
def test_empty_input(engine):
    df = pd.DataFrame(columns=["ts", "price", "volume"])

    out = engine.run(df)

    assert out.empty
    assert list(out.columns) == [
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "turnover",
        "trade_count",
    ]
def test_determinism(engine, simple_trade_df):
    out1 = engine.run(simple_trade_df)
    out2 = engine.run(simple_trade_df)

    pd.testing.assert_frame_equal(out1, out2)
