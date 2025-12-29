from __future__ import annotations

from typing import Dict, Tuple

import pytest
import pyarrow as pa
import pyarrow.compute as pc

from src.engines.normalize_engine import NormalizeEngine, NormalizeResult


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------
@pytest.fixture
def engine() -> NormalizeEngine:
    return NormalizeEngine()


@pytest.fixture
def raw_trade_table() -> pa.Table:
    """
    模拟 parse 之后、NormalizeEngine 之前的最小 Arrow 输入
    已满足 NormalizeEngine 契约：
      - symbol
      - ts
    """
    return pa.table(
        {
            "symbol": [
                "600000",
                "600000",
                "000001",
                "300001",
                "000001",
            ],
            "ts": [
                2025120193002000,
                2025120193001000,
                2025120193003000,
                2025120193002001,
                2025120193002000,
            ],
            "price": [10.1, 10.0, 9.9, 20.0, 10.2],
        }
    )


# -----------------------------------------------------------------------------
# execute() 基础语义
# -----------------------------------------------------------------------------
def test_normalize_basic(engine: NormalizeEngine, raw_trade_table: pa.Table):
    result = engine.execute([raw_trade_table])

    assert isinstance(result, NormalizeResult)
    assert isinstance(result.canonical, pa.Table)
    assert isinstance(result.index, dict)
    assert result.rows == result.canonical.num_rows


def test_normalize_empty_input(engine: NormalizeEngine):
    result = engine.execute([])

    assert result.rows == 0
    assert result.index == {}
    assert result.canonical.num_rows == 0


def test_normalize_ignores_empty_tables(engine: NormalizeEngine):
    empty = pa.table({"symbol": [], "ts": []})
    result = engine.execute([empty])

    assert result.rows == 0
    assert result.index == {}


# -----------------------------------------------------------------------------
# 排序语义：(symbol asc, ts asc)
# -----------------------------------------------------------------------------
def test_normalize_sorted_by_symbol_then_ts(
    engine: NormalizeEngine,
    raw_trade_table: pa.Table,
):
    result = engine.execute([raw_trade_table])
    table = result.canonical

    symbols = table["symbol"].to_pylist()
    ts = table["ts"].to_pylist()

    for i in range(1, len(symbols)):
        if symbols[i] == symbols[i - 1]:
            assert ts[i] >= ts[i - 1]
        else:
            assert symbols[i] > symbols[i - 1]


# -----------------------------------------------------------------------------
# index 语义
# -----------------------------------------------------------------------------
def test_index_covers_all_rows(
    engine: NormalizeEngine,
    raw_trade_table: pa.Table,
):
    result = engine.execute([raw_trade_table])

    total = sum(length for _, length in result.index.values())
    assert total == result.rows


def test_index_slice_correctness(
    engine: NormalizeEngine,
    raw_trade_table: pa.Table,
):
    result = engine.execute([raw_trade_table])
    table = result.canonical

    for symbol, (start, length) in result.index.items():
        sliced = table.slice(start, length)
        slice_symbols = set(sliced["symbol"].to_pylist())
        assert slice_symbols == {symbol}


# -----------------------------------------------------------------------------
# build_symbol_slice_index（保持你原有测试）
# -----------------------------------------------------------------------------
def build_index_python(table: pa.Table) -> Dict[str, Tuple[int, int]]:
    symbols = table["symbol"].to_pylist()
    index: Dict[str, Tuple[int, int]] = {}

    start = 0
    cur = symbols[0]
    for i in range(1, len(symbols)):
        if symbols[i] != cur:
            index[cur] = (start, i - start)
            cur = symbols[i]
            start = i
    index[cur] = (start, len(symbols) - start)
    return index


def test_build_symbol_slice_index_matches_python():
    table = pa.table(
        {
            "symbol": ["000001", "000001", "000002", "000002", "000002", "000003"],
            "ts": [1, 2, 1, 2, 3, 1],
        }
    )

    index_arrow = NormalizeEngine.build_symbol_slice_index(table)
    index_py = build_index_python(table)

    assert index_arrow == index_py


def test_build_symbol_slice_index_semantics():
    table = pa.table(
        {
            "symbol": ["000001", "000001", "000002", "000002", "000003"],
            "ts": [1, 2, 1, 2, 1],
        }
    )

    index = NormalizeEngine.build_symbol_slice_index(table)

    for sym, (start, length) in index.items():
        slice_sym = table["symbol"].slice(start, length)
        assert pc.all(pc.equal(slice_sym, sym)).as_py()


# -----------------------------------------------------------------------------
# filter_a_share_arrow（从原 normalize 测试中拆出）
# -----------------------------------------------------------------------------
def test_filter_a_share_arrow():
    engine = NormalizeEngine()

    table = pa.table(
        {
            "SecurityID": [
                "600000",
                "000001",
                "300001",
                "688001",
                "AAPL",
                "GOOG",
            ],
            "x": [1, 2, 3, 4, 5, 6],
        }
    )

    out = engine.filter_a_share_arrow(table)
    symbols = set(out["SecurityID"].to_pylist())

    assert symbols == {"600000", "000001", "300001", "688001"}


def test_filter_a_share_empty_table():
    engine = NormalizeEngine()
    empty = pa.table({"SecurityID": []})

    out = engine.filter_a_share_arrow(empty)
    assert out.num_rows == 0


def test_filter_a_share_missing_column_raises():
    engine = NormalizeEngine()
    table = pa.table({"x": [1, 2, 3]})

    with pytest.raises(ValueError):
        engine.filter_a_share_arrow(table)
