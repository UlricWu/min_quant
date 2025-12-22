from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.engines.symbol_split_engine import SymbolSplitEngine


# ------------------------------------------------------------
# helper: 构造 canonical table
# ------------------------------------------------------------
def make_table(rows):
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("ts", pa.int64()),
            ("price", pa.float64()),
            ("volume", pa.int64()),
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


# ============================================================
# 1. split_one：基础正确性
# ============================================================
def test_split_one_basic():
    table = make_table(
        [
            {"symbol": "AAA", "ts": 1, "price": 10.0, "volume": 100},
            {"symbol": "BBB", "ts": 2, "price": 20.0, "volume": 200},
            {"symbol": "AAA", "ts": 3, "price": 11.0, "volume": 150},
        ]
    )

    engine = SymbolSplitEngine()
    buf = engine.split_one(table, "AAA")

    assert isinstance(buf, (bytes, bytearray))

    sub = pq.read_table(pa.BufferReader(buf))
    assert sub.num_rows == 2
    assert set(sub["symbol"].to_pylist()) == {"AAA"}


# ============================================================
# 2. split_one：symbol 不存在
# ============================================================
def test_split_one_symbol_not_found():
    table = make_table(
        [
            {"symbol": "AAA", "ts": 1, "price": 10.0, "volume": 100},
        ]
    )

    engine = SymbolSplitEngine()
    buf = engine.split_one(table, "BBB")

    sub = pq.read_table(pa.BufferReader(buf))
    assert sub.num_rows == 0


# ============================================================
# 3. split_many：多个 symbol
# ============================================================
def test_split_many_basic():
    table = make_table(
        [
            {"symbol": "AAA", "ts": 1, "price": 10.0, "volume": 100},
            {"symbol": "BBB", "ts": 2, "price": 20.0, "volume": 200},
            {"symbol": "AAA", "ts": 3, "price": 11.0, "volume": 150},
            {"symbol": "CCC", "ts": 4, "price": 30.0, "volume": 300},
            {"symbol": "BBB", "ts": 5, "price": 21.0, "volume": 220},
        ]
    )

    engine = SymbolSplitEngine()
    result = engine.split_many(table)

    assert set(result.keys()) == {"AAA", "BBB", "CCC"}

    assert result["AAA"].num_rows == 2
    assert result["BBB"].num_rows == 2
    assert result["CCC"].num_rows == 1

    assert set(result["AAA"]["symbol"].to_pylist()) == {"AAA"}
    assert set(result["BBB"]["symbol"].to_pylist()) == {"BBB"}


# ============================================================
# 4. split_many：内容一致性（集合意义）
# ============================================================
def test_split_many_content_integrity():
    rows = [
        {"symbol": "AAA", "ts": 1, "price": 10.0, "volume": 100},
        {"symbol": "AAA", "ts": 2, "price": 11.0, "volume": 110},
        {"symbol": "BBB", "ts": 3, "price": 20.0, "volume": 200},
    ]

    table = make_table(rows)
    engine = SymbolSplitEngine()
    result = engine.split_many(table)

    merged = []
    for sym, sub in result.items():
        merged.extend(sub.to_pylist())

    # 排序后逐行比较
    assert sorted(merged, key=lambda x: (x["symbol"], x["ts"])) == \
           sorted(rows, key=lambda x: (x["symbol"], x["ts"]))


# ============================================================
# 5. split_many：空表
# ============================================================
def test_split_many_empty_table():
    table = make_table([])

    engine = SymbolSplitEngine()
    result = engine.split_many(table)

    assert result == {}


# ============================================================
# 6. split_many：缺少 symbol 列
# ============================================================
def test_split_many_missing_symbol_column():
    schema = pa.schema(
        [
            ("ts", pa.int64()),
            ("price", pa.float64()),
        ]
    )
    table = pa.Table.from_pylist(
        [{"ts": 1, "price": 10.0}],
        schema=schema,
    )

    engine = SymbolSplitEngine()

    with pytest.raises(KeyError):
        engine.split_many(table)


# ============================================================
# 7. 自定义 symbol 字段名
# ============================================================
def test_split_custom_symbol_field():
    schema = pa.schema(
        [
            ("code", pa.string()),
            ("ts", pa.int64()),
        ]
    )

    table = pa.Table.from_pylist(
        [
            {"code": "X", "ts": 1},
            {"code": "Y", "ts": 2},
            {"code": "X", "ts": 3},
        ],
        schema=schema,
    )

    engine = SymbolSplitEngine(symbol_field="code")
    result = engine.split_many(table)

    assert set(result.keys()) == {"X", "Y"}
    assert result["X"].num_rows == 2
