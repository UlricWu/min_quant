import pytest
import pyarrow as pa

from src.engines.symbol_router_engine import SymbolRouterEngine

def test_split_basic():
    engine = SymbolRouterEngine()

    table = pa.table(
        {
            "Symbol": ["000001", "000002", "000001", "000002"],
            "price": ["10", "20", "11", "21"],
        }
    )

    out = engine.split(table)

    assert set(out.keys()) == {"000001", "000002"}

    t1 = out["000001"]
    t2 = out["000002"]

    assert t1["price"].to_pylist() == ["10", "11"]
    assert t2["price"].to_pylist() == ["20", "21"]

def test_split_accepts_recordbatch():
    engine = SymbolRouterEngine()

    batch = pa.record_batch(
        [
            pa.array(["000001", "000001", "000002"]),
            pa.array(["1", "2", "3"]),
        ],
        names=["Symbol", "qty"],
    )

    out = engine.split(batch)

    assert set(out.keys()) == {"000001", "000002"}
    assert out["000001"]["qty"].to_pylist() == ["1", "2"]
    assert out["000002"]["qty"].to_pylist() == ["3"]

def test_schema_preserved():
    engine = SymbolRouterEngine()

    table = pa.table(
        {
            "Symbol": ["000001", "000002"],
            "a": ["x", "y"],
            "b": ["1", "2"],
        }
    )

    out = engine.split(table)

    for sub in out.values():
        assert sub.schema == table.schema

def test_row_order_preserved():
    engine = SymbolRouterEngine()

    table = pa.table(
        {
            "Symbol": ["A", "B", "A", "A", "B"],
            "seq": ["1", "2", "3", "4", "5"],
        }
    )

    out = engine.split(table)

    assert out["A"]["seq"].to_pylist() == ["1", "3", "4"]
    assert out["B"]["seq"].to_pylist() == ["2", "5"]
def test_none_symbol_rows_ignored():
    engine = SymbolRouterEngine()

    table = pa.table(
        {
            "Symbol": ["000001", None, "000001"],
            "v": ["a", "b", "c"],
        }
    )

    out = engine.split(table)

    assert set(out.keys()) == {"000001"}
    assert out["000001"]["v"].to_pylist() == ["a", "c"]
def test_missing_symbol_column_raises():
    engine = SymbolRouterEngine()

    table = pa.table(
        {
            "price": ["10", "11"],
        }
    )

    with pytest.raises(ValueError, match="Symbol"):
        engine.split(table)
def test_symbol_must_be_string():
    engine = SymbolRouterEngine()

    table = pa.table(
        {
            "Symbol": pa.array([1, 2], type=pa.int32()),
            "v": ["a", "b"],
        }
    )

    with pytest.raises(TypeError):
        engine.split(table)
def test_no_symbol_whitelist_filtering():
    engine = SymbolRouterEngine()

    table = pa.table(
        {
            "Symbol": ["000001", "999999"],
            "x": ["a", "b"],
        }
    )

    out = engine.split(table)

    assert set(out.keys()) == {"000001", "999999"}
def test_deterministic_output():
    engine = SymbolRouterEngine()

    table = pa.table(
        {
            "Symbol": ["A", "B", "A"],
            "v": ["1", "2", "3"],
        }
    )

    out1 = engine.split(table)
    out2 = engine.split(table)

    assert out1.keys() == out2.keys()
    for k in out1:
        assert out1[k].to_pydict() == out2[k].to_pydict()
