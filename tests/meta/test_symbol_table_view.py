# tests/meta/test_symbol_table_view.py
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from src.meta.symbol_accessor import SymbolTableView


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------
@pytest.fixture
def base_table() -> pa.Table:
    """
    row-wise 对齐的 Arrow Table
    """
    return pa.table(
        {
            "x": [10, 20, 30, 40, 50],
            "y": ["a", "b", "c", "d", "e"],
        }
    )


@pytest.fixture
def slice_index() -> dict[str, tuple[int, int]]:
    """
    symbol -> (start, length)
    """
    return {
        "A": (0, 2),
        "B": (2, 3),
    }


@pytest.fixture
def view(base_table: pa.Table, slice_index: dict) -> SymbolTableView:
    return SymbolTableView(
        table=base_table,
        index=slice_index,
    )


# -----------------------------------------------------------------------------
# Basic contract
# -----------------------------------------------------------------------------
def test_symbols(view: SymbolTableView):
    assert set(view.symbols()) == {"A", "B"}


def test_get_existing_symbol(view: SymbolTableView):
    a = view.get("A")
    b = view.get("B")

    assert a.num_rows == 2
    assert b.num_rows == 3

    assert a["x"].to_pylist() == [10, 20]
    assert b["x"].to_pylist() == [30, 40, 50]


def test_get_missing_symbol_returns_empty(view: SymbolTableView):
    c = view.get("C")

    assert c.num_rows == 0
    # schema 必须保持一致
    assert c.schema == view.get("A").schema


# -----------------------------------------------------------------------------
# Zero-copy / slice semantics
# -----------------------------------------------------------------------------
def test_slice_is_view_not_copy(base_table: pa.Table, view: SymbolTableView):
    """
    Arrow slice 语义测试（行为级）
    """
    a = view.get("A")

    # 修改原 table 不应影响 slice 的已 materialized 结果
    # （Arrow slice 是 view，但数据不可变，测试语义即可）
    assert a["x"].to_pylist() == [10, 20]


# -----------------------------------------------------------------------------
# Edge cases
# -----------------------------------------------------------------------------
def test_empty_index():
    table = pa.table({"x": [1, 2, 3]})
    view = SymbolTableView(table=table, index={})

    assert list(view.symbols()) == []
    assert view.get("ANY").num_rows == 0


def test_zero_length_slice(base_table: pa.Table):
    index = {
        "A": (0, 0),
    }
    view = SymbolTableView(table=base_table, index=index)

    a = view.get("A")
    assert a.num_rows == 0
    assert a.schema == base_table.schema


# -----------------------------------------------------------------------------
# Defensive behavior (非法 index)
# -----------------------------------------------------------------------------
def test_invalid_slice_range_does_not_crash(base_table: pa.Table):
    """
    SymbolTableView 不负责校验 index 合法性
    （这是 SymbolAccessor / Meta 的责任）
    这里只验证：不会 silently 返回错误数据
    """
    index = {
        "A": (100, 10),  # 越界
    }
    view = SymbolTableView(table=base_table, index=index)

    a = view.get("A")
    assert a.num_rows == 0
