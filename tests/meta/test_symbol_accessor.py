# tests/meta/test_symbol_accessor.py
from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.meta.symbol_accessor import SymbolAccessor, SymbolTableView


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------
@pytest.fixture
def manifest_with_slice(tmp_path: Path) -> Path:
    """
    构造一个带 symbol_slice index 的通用 manifest
    （不依赖 stage）
    """
    table = pa.table(
        {
            "symbol": ["A", "A", "B", "B", "B"],
            "price": [1, 2, 3, 4, 5],
        }
    )

    parquet_path = tmp_path / "canonical.parquet"
    pq.write_table(table, parquet_path)

    manifest = {
        "stage": "any_stage_is_ok",
        "outputs": {
            "file": str(parquet_path),
            "rows": 5,
            "index": {
                "type": "symbol_slice",
                "symbols": {
                    "A": [0, 2],
                    "B": [2, 3],
                },
            },
        },
    }

    manifest_path = tmp_path / "data.manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


# -----------------------------------------------------------------------------
# SymbolAccessor.from_manifest
# -----------------------------------------------------------------------------
def test_symbol_accessor_basic(manifest_with_slice: Path):
    accessor = SymbolAccessor.from_manifest(manifest_with_slice)

    assert accessor.rows == 5
    assert set(accessor.symbols()) == {"A", "B"}


def test_symbol_slice(manifest_with_slice: Path):
    accessor = SymbolAccessor.from_manifest(manifest_with_slice)

    a = accessor.get("A")
    b = accessor.get("B")
    c = accessor.get("C")  # 不存在

    assert a.num_rows == 2
    assert b.num_rows == 3
    assert c.num_rows == 0


def test_symbol_slice_schema_preserved(manifest_with_slice: Path):
    accessor = SymbolAccessor.from_manifest(manifest_with_slice)

    full = accessor.table
    sliced = accessor.get("A")

    assert sliced.schema == full.schema


# -----------------------------------------------------------------------------
# Error cases (manifest contract)
# -----------------------------------------------------------------------------
def test_missing_outputs_index(tmp_path: Path):
    manifest = {
        "outputs": {
            "file": "dummy.parquet",
        }
    }

    p = tmp_path / "bad.manifest.json"
    p.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError):
        SymbolAccessor.from_manifest(p)


def test_missing_parquet_file(tmp_path: Path):
    manifest = {
        "outputs": {
            "file": str(tmp_path / "not_exist.parquet"),
            "index": {
                "type": "symbol_slice",
                "symbols": {"A": [0, 1]},
            },
        }
    }

    p = tmp_path / "bad.manifest.json"
    p.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(FileNotFoundError):
        SymbolAccessor.from_manifest(p)


# -----------------------------------------------------------------------------
# SymbolTableView (bind)
# -----------------------------------------------------------------------------
def test_symbol_table_view_bind(manifest_with_slice: Path):
    accessor = SymbolAccessor.from_manifest(manifest_with_slice)

    # 构造 row-wise 对齐的外部 table
    ext = pa.table(
        {
            "x": [10, 20, 30, 40, 50],
        }
    )

    view = accessor.bind(ext)

    a = view.get("A")
    b = view.get("B")

    assert a.num_rows == 2
    assert b.num_rows == 3
    assert a["x"].to_pylist() == [10, 20]
    assert b["x"].to_pylist() == [30, 40, 50]


def test_symbol_table_view_bind_row_mismatch(manifest_with_slice: Path):
    accessor = SymbolAccessor.from_manifest(manifest_with_slice)

    bad = pa.table(
        {
            "x": [1, 2, 3],  # 行数不一致
        }
    )

    with pytest.raises(ValueError):
        accessor.bind(bad)
