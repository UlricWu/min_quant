# tests/engines/test_symbol_index_engine.py
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from src.engines.symbol_index_engine import SymbolIndexEngine


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def table_from_rows(rows: list[dict]) -> pa.Table:
    """
    构造 Arrow Table（schema 自动推断）
    """
    if not rows:
        return pa.table({})
    return pa.table({k: [r[k] for r in rows] for k in rows[0]})


# -----------------------------------------------------------------------------
# tests
# -----------------------------------------------------------------------------
def test_symbol_index_basic_sort_and_index():
    """
    基础功能：
    - 无序输入
    - 多 symbol
    - ts 排序
    - index 正确
    """
    table = table_from_rows(
        [
            {"symbol": "B", "ts": 3},
            {"symbol": "A", "ts": 2},
            {"symbol": "A", "ts": 1},
            {"symbol": "B", "ts": 1},
        ]
    )

    sorted_table, index = SymbolIndexEngine.execute(table)

    assert sorted_table["symbol"].to_pylist() == ["A", "A", "B", "B"]
    assert sorted_table["ts"].to_pylist() == [1, 2, 1, 3]

    assert index == {
        "A": (0, 2),
        "B": (2, 2),
    }


def test_symbol_index_single_symbol():
    """
    单一 symbol
    """
    table = table_from_rows(
        [
            {"symbol": "A", "ts": 3},
            {"symbol": "A", "ts": 1},
            {"symbol": "A", "ts": 2},
        ]
    )

    sorted_table, index = SymbolIndexEngine.execute(table)

    assert sorted_table["ts"].to_pylist() == [1, 2, 3]
    assert index == {"A": (0, 3)}


def test_symbol_index_empty_table():
    """
    空表输入
    """
    table = pa.table({})

    sorted_table, index = SymbolIndexEngine.execute(table)

    assert sorted_table.num_rows == 0
    assert index == {}


def test_symbol_index_dictionary_symbol():
    """
    dictionary-encoded symbol（Arrow 常见情况）
    """
    symbols = pa.array(["A", "B", "A", "C"]).dictionary_encode()
    ts = pa.array([3, 1, 2, 1], pa.int64())

    table = pa.Table.from_arrays([symbols, ts], names=["symbol", "ts"])

    sorted_table, index = SymbolIndexEngine.execute(table)

    assert sorted_table["symbol"].to_pylist() == ["A", "A", "B", "C"]
    assert sorted_table["ts"].to_pylist() == [2, 3, 1, 1]

    assert index == {
        "A": (0, 2),
        "B": (2, 1),
        "C": (3, 1),
    }


def test_symbol_index_invalid_symbol_type():
    """
    非 string / dictionary 的 symbol 应报错
    """
    table = table_from_rows(
        [
            {"symbol": 1, "ts": 1},
            {"symbol": 2, "ts": 2},
        ]
    )

    with pytest.raises(TypeError):
        SymbolIndexEngine.execute(table)
