import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.meta.symbol_accessor import SymbolAccessor

@pytest.fixture
def normalize_manifest(tmp_path: Path) -> Path:
    table = pa.table(
        {
            "symbol": ["A", "A", "B", "B", "B"],
            "price": [1, 2, 3, 4, 5],
        }
    )

    parquet_path = tmp_path / "canonical.parquet"
    pq.write_table(table, parquet_path)

    manifest = {
        "stage": "normalize",
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

    manifest_path = tmp_path / "normalize.manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    return manifest_path

def test_symbol_accessor_from_manifest(normalize_manifest: Path):
    accessor = SymbolAccessor.from_manifest(normalize_manifest)

    assert accessor.rows == 5
    assert set(accessor.symbols()) == {"A", "B"}

def test_symbol_slice(normalize_manifest: Path):
    accessor = SymbolAccessor.from_manifest(normalize_manifest)

    a = accessor.get("A")
    b = accessor.get("B")
    c = accessor.get("C")  # 不存在

    assert a.num_rows == 2
    assert b.num_rows == 3
    assert c.num_rows == 0

def test_symbol_slice_schema(normalize_manifest: Path):
    accessor = SymbolAccessor.from_manifest(normalize_manifest)

    full = accessor.table
    sliced = accessor.get("A")

    assert sliced.schema == full.schema

def test_invalid_manifest_stage(tmp_path: Path):
    bad_manifest = {
        "stage": "csv_convert",
        "outputs": {},
    }

    p = tmp_path / "bad.manifest.json"
    p.write_text(json.dumps(bad_manifest), encoding="utf-8")

    with pytest.raises(ValueError):
        SymbolAccessor.from_manifest(p)
