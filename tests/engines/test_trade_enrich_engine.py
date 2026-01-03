import pyarrow as pa
import pytest

from src.data_system.engines.trade_enrich_engine import TradeEnrichEngine

def test_trade_enrich_basic():
    table = pa.table(
        {
            "price": [10.0, 10.5, 10.5, 10.2],
            "volume": [100, 50, 20, 10],
        }
    )

    engine = TradeEnrichEngine()
    out = engine.execute(table)

    # 行数不变
    assert out.num_rows == 4

    # notional
    assert out["notional"].to_pylist() == [
        1000.0,
        525.0,
        210.0,
        102.0,
    ]

    # trade_side (tick rule)
    # 第 0 行 → 0
    # 10.5 > 10.0 → +1
    # 10.5 == 10.5 → 0
    # 10.2 < 10.5 → -1
    assert out["trade_side"].to_pylist() == [0, 1, 0, -1]

def test_trade_enrich_first_row_side_is_zero():
    table = pa.table(
        {
            "price": [10.0, 10.1],
            "volume": [100, 50],
        }
    )

    out = TradeEnrichEngine().execute(table)

    assert out["trade_side"].to_pylist()[0] == 0


def test_trade_enrich_empty_table():
    table = pa.table(
        {
            "price": pa.array([], pa.float64()),
            "volume": pa.array([], pa.int64()),
        }
    )

    engine = TradeEnrichEngine()
    out = engine.execute(table)

    assert out.num_rows == 0
    assert out.schema == table.schema
def test_trade_enrich_missing_columns():
    table = pa.table(
        {
            "price": [10.0, 10.1],
        }
    )

    engine = TradeEnrichEngine()

    with pytest.raises(ValueError) as exc:
        engine.execute(table)

    assert "missing required columns" in str(exc.value)
def test_trade_enrich_chunked_array():
    price = pa.chunked_array(
        [[10.0, 10.5], [10.3]],
        type=pa.float64(),
    )
    volume = pa.chunked_array(
        [[100, 50], [20]],
        type=pa.int64(),
    )

    table = pa.table(
        {
            "price": price,
            "volume": volume,
        }
    )

    engine = TradeEnrichEngine()
    out = engine.execute(table)

    assert out["notional"].to_pylist() == [1000.0, 525.0, 206.0]
    assert out["trade_side"].to_pylist() == [0, 1, -1]
def test_trade_enrich_idempotent():
    table = pa.table(
        {
            "price": [10.0, 10.1],
            "volume": [100, 50],
        }
    )

    engine = TradeEnrichEngine()

    out1 = engine.execute(table)
    out2 = engine.execute(out1)

    # 列名不应重复
    assert out2.column_names.count("notional") == 1
    assert out2.column_names.count("trade_side") == 1

    # 数值保持一致
    assert out2["notional"].to_pylist() == [1000.0, 505.0]
    assert out2["trade_side"].to_pylist() == [0, 1]
def test_trade_enrich_custom_column_names():
    table = pa.table(
        {
            "p": [10.0, 9.8],
            "v": [100, 50],
        }
    )

    engine = TradeEnrichEngine(
        price_col="p",
        volume_col="v",
        notional_col="amt",
        side_col="side",
    )

    out = engine.execute(table)

    assert "amt" in out.column_names
    assert "side" in out.column_names

    import pytest

    assert out["amt"].to_pylist() == pytest.approx(
        [1000.0, 490.0],
        rel=1e-12,
    )

    assert out["side"].to_pylist() == [0, -1]
