# tests/engines/test_normalize_engine.py
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from src.engines.normalize_engine import NormalizeEngine


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def table_from_rows(rows: list[dict]) -> pa.Table:
    if not rows:
        return pa.table({})
    return pa.table({k: [r[k] for r in rows] for k in rows[0]})


# -----------------------------------------------------------------------------
# basic behavior
# -----------------------------------------------------------------------------
def test_normalize_sort_basic():
    engine = NormalizeEngine()

    table = table_from_rows(
        [
            {"symbol": "600000", "ts": 3},
            {"symbol": "000001", "ts": 2},
            {"symbol": "000001", "ts": 1},
            {"symbol": "600000", "ts": 1},
        ]
    )

    out = engine.execute(table)

    assert out["symbol"].to_pylist() == [
        "000001", "000001", "600000", "600000"
    ]
    assert out["ts"].to_pylist() == [1, 2, 1, 3]


def test_normalize_single_row():
    engine = NormalizeEngine()

    table = table_from_rows(
        [{"symbol": "600000", "ts": 100}]
    )

    out = engine.execute(table)

    assert out.num_rows == 1
    assert out["symbol"].to_pylist() == ["600000"]
    assert out["ts"].to_pylist() == [100]


# -----------------------------------------------------------------------------
# dictionary symbol
# -----------------------------------------------------------------------------
def test_normalize_dictionary_symbol():
    engine = NormalizeEngine()

    symbols = pa.array(
        ["000001", "000001", "600000"]
    ).dictionary_encode()
    ts = pa.array([3, 2, 1], pa.int64())

    table = pa.Table.from_arrays([symbols, ts], names=["symbol", "ts"])

    out = engine.execute(table)

    assert out["symbol"].to_pylist() == ["000001", "000001", "600000"]
    assert out["ts"].to_pylist() == [2, 3, 1]


# -----------------------------------------------------------------------------
# empty / none
# -----------------------------------------------------------------------------
def test_normalize_empty_table():
    engine = NormalizeEngine()

    table = pa.table({})
    out = engine.execute(table)

    assert out.num_rows == 0


def test_normalize_none():
    engine = NormalizeEngine()

    out = engine.execute(None)

    assert out is None


# -----------------------------------------------------------------------------
# validation: missing columns
# -----------------------------------------------------------------------------
def test_normalize_missing_symbol():
    engine = NormalizeEngine()

    table = table_from_rows(
        [{"ts": 1}]
    )

    with pytest.raises(ValueError, match="missing required columns"):
        engine.execute(table)


def test_normalize_missing_ts():
    engine = NormalizeEngine()

    table = table_from_rows(
        [{"symbol": "A"}]
    )

    with pytest.raises(ValueError, match="missing required columns"):
        engine.execute(table)


# -----------------------------------------------------------------------------
# validation: invalid types
# -----------------------------------------------------------------------------
def test_normalize_invalid_symbol_type():
    engine = NormalizeEngine()

    table = table_from_rows(
        [{"symbol": 123, "ts": 1}]
    )

    with pytest.raises(TypeError, match="column 'symbol'"):
        engine.execute(table)


def test_normalize_invalid_ts_type():
    engine = NormalizeEngine()

    table = table_from_rows(
        [{"symbol": "A", "ts": "bad"}]
    )

    with pytest.raises(TypeError, match="column 'ts'"):
        engine.execute(table)


# -----------------------------------------------------------------------------
# 5. A-share 过滤语义
# -----------------------------------------------------------------------------
def test_filter_a_share_basic():
    engine = NormalizeEngine()

    table = pa.table(
        {
            "symbol": [
                "600000",  # SH
                "000001",  # SZ
                "300001",  # SZ
                "688001",  # SH
                "AAPL",  # 非 A 股
            ],
            "ts": [1, 2, 3, 4, 5],
        }
    )

    out = engine.execute(table)

    symbols = set(out["symbol"].to_pylist())
    assert symbols == {"600000", "000001", "300001", "688001"}


def test_filter_a_share_all_filtered():
    engine = NormalizeEngine()

    table = table_from_rows(
        [
            {"symbol": "900001", "ts": 1},
            {"symbol": "200001", "ts": 2},
        ]
    )

    out = engine.filter_a_share_arrow(table)

    assert out.num_rows == 0


def test_filter_a_share_missing_symbol():
    engine = NormalizeEngine()

    table = table_from_rows(
        [{"ts": 1}]
    )

    with pytest.raises(ValueError, match="missing column"):
        engine.filter_a_share_arrow(table)


# -----------------------------------------------------------------------------
# 4. 排序语义（symbol asc, ts asc）
# -----------------------------------------------------------------------------
def test_sort_basic_symbol_then_ts():
    engine = NormalizeEngine()

    table = table_from_rows(
        [
            {"symbol": "600001", "ts": 3},
            {"symbol": "300001", "ts": 2},
            {"symbol": "300001", "ts": 1},
            {"symbol": "600001", "ts": 1},
        ]
    )

    out = engine.execute(table)

    assert out["symbol"].to_pylist() == ["300001", "300001", "600001", "600001"]
    assert out["ts"].to_pylist() == [1, 2, 1, 3]


def test_sort_is_stable_within_same_symbol():
    engine = NormalizeEngine()

    table = table_from_rows(
        [
            {"symbol": "300001", "ts": 3, "x": 1},
            {"symbol": "300001", "ts": 1, "x": 2},
            {"symbol": "300001", "ts": 2, "x": 3},
        ]
    )

    out = engine.execute(table)

    assert out["ts"].to_pylist() == [1, 2, 3]
    assert out["x"].to_pylist() == [2, 3, 1]
# -----------------------------------------------------------------------------
# 6. 行为边界（冻结约束）
# -----------------------------------------------------------------------------
def test_execute_does_not_add_or_remove_columns():
    engine = NormalizeEngine()

    table = table_from_rows(
        [
            {"symbol": "A", "ts": 2, "price": 10.0},
            {"symbol": "A", "ts": 1, "price": 9.9},
        ]
    )

    out = engine.execute(table)

    assert out.column_names == ["symbol", "ts", "price"]


def test_execute_returns_new_table_object():
    engine = NormalizeEngine()

    table = table_from_rows(
        [
            {"symbol": "A", "ts": 2},
            {"symbol": "A", "ts": 1},
        ]
    )

    out = engine.execute(table)

    # Arrow Table 是不可变的，但我们仍然要求语义上是“新对象”
    assert out is not table