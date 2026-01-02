from __future__ import annotations

import pytest
import pyarrow as pa

from src.engines.normalize_engine import NormalizeEngine
from src.engines.parser_engine import INTERNAL_SCHEMA


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def make_internal_trade_table() -> pa.Table:
    """
    构造一个满足 INTERNAL_SCHEMA 的最小 internal 表
    含乱序 (symbol, ts)，用于验证 NormalizeEngine 的单 batch 行为
    """
    n = 4

    symbol_vals = ["600000", "600000", "000001", "300001"]
    ts_vals = [
        2025120193002000,
        2025120193001000,
        2025120193003000,
        2025120193002000,
    ]

    arrays = []
    for field in INTERNAL_SCHEMA:
        if field.name == "symbol":
            arrays.append(pa.array(symbol_vals, type=field.type))
        elif field.name == "ts":
            arrays.append(pa.array(ts_vals, type=field.type))
        else:
            arrays.append(pa.array([None] * n, type=field.type))

    return pa.Table.from_arrays(arrays, schema=INTERNAL_SCHEMA)


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
def test_normalize_basic_sort_and_filter():
    engine = NormalizeEngine()
    table = make_internal_trade_table()

    out = engine.execute(table)

    assert out.num_rows > 0
    assert out.column_names == table.column_names

    # A-share filter 生效（无非 A 股）
    symbols = out["symbol"].to_pylist()
    assert set(symbols) <= {"600000", "000001", "300001"}

    # 排序语义：(symbol asc, ts asc)
    prev_sym = None
    prev_ts = None
    for sym, ts in zip(out["symbol"].to_pylist(), out["ts"].to_pylist()):
        if prev_sym is None:
            prev_sym, prev_ts = sym, ts
            continue

        if sym == prev_sym:
            assert ts >= prev_ts
        else:
            assert sym > prev_sym

        prev_sym, prev_ts = sym, ts


def test_normalize_dictionary_symbol_cast():
    engine = NormalizeEngine()

    symbols = pa.array(
        ["000001", "000001", "600000"],
    ).dictionary_encode()
    ts = pa.array([3, 1, 2], pa.int64())

    table = pa.Table.from_arrays([symbols, ts], names=["symbol", "ts"])
    out = engine.execute(table)

    assert pa.types.is_string(out["symbol"].type)
    assert out["symbol"].to_pylist() == ["000001", "000001", "600000"]


def test_normalize_empty_table_passthrough():
    engine = NormalizeEngine()
    table = pa.table({"symbol": [], "ts": []})

    out = engine.execute(table)

    assert out.num_rows == 0
    assert out.column_names == table.column_names


def test_normalize_missing_required_columns_raises():
    engine = NormalizeEngine()

    with pytest.raises(ValueError):
        engine.execute(pa.table({"symbol": ["000001"]}))

    with pytest.raises(ValueError):
        engine.execute(pa.table({"ts": [1, 2, 3]}))


def test_normalize_invalid_symbol_type_raises():
    engine = NormalizeEngine()

    table = pa.table(
        {
            "symbol": pa.array([1, 2, 3], pa.int64()),
            "ts": [1, 2, 3],
        }
    )

    with pytest.raises(TypeError):
        engine.execute(table)
