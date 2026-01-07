# tests/meta/test_slice_source_contract.py
from __future__ import annotations

from pathlib import Path
import json

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.meta.slice_source import SliceSource


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------
@pytest.fixture
def meta_dir_with_symbol_slice(tmp_path: Path) -> Path:
    """
    构造一个最小可用的 meta_dir：
      meta/<stage>/<output_slot>/manifest.json
    """
    meta_dir = tmp_path / "meta"
    stage = "any_stage"
    slot = "canonical"

    manifest_dir = meta_dir / stage / slot
    manifest_dir.mkdir(parents=True)

    # --------------------------------------------------
    # parquet
    # --------------------------------------------------
    table = pa.table(
        {
            "symbol": ["A", "A", "B", "B", "B"],
            "price": [1, 2, 3, 4, 5],
        }
    )

    parquet_path = manifest_dir / "data_handler.parquet"
    pq.write_table(table, parquet_path)

    # --------------------------------------------------
    # manifest.json
    # --------------------------------------------------
    manifest = {
        "outputs": {
            "file": str(parquet_path),
            "rows": 5,
            "index": {
                "type": "symbol_slice",
                "symbols": {
                    "A": [0, 2],
                    "B": [2, 5],
                },
            },
        }
    }
    manifest_path = meta_dir / f"{stage}.{slot}.manifest.json"

    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    return meta_dir


@pytest.fixture
def slice_source(meta_dir_with_symbol_slice: Path) -> SliceSource:
    return SliceSource(
        meta_dir=meta_dir_with_symbol_slice,
        stage="any_stage",
        output_slot="canonical",
    )


# -----------------------------------------------------------------------------
# Contract Tests
# -----------------------------------------------------------------------------
def test_slice_source_symbols(slice_source: SliceSource):
    """
    Contract:
    - symbols() 暴露所有 slice key
    """
    assert set(slice_source.symbols()) == {"A", "B"}


def test_slice_source_get_returns_table(slice_source: SliceSource):
    """
    Contract:
    - get(symbol) -> Arrow Table
    """
    t = slice_source.get("A")

    assert isinstance(t, pa.Table)
    assert t.num_rows == 2
    assert t.column("symbol").to_pylist() == ["A", "A"]


def test_slice_source_iter(slice_source: SliceSource):
    """
    Contract:
    - SliceSource 可直接迭代
    """
    result = {sym: tbl.num_rows for sym, tbl in slice_source}

    assert result == {
        "A": 2,
        "B": 3,
    }


def test_slice_source_missing_symbol_raises(slice_source: SliceSource):
    """
    Contract:
    - 请求不存在的 symbol → KeyError（由 accessor 传播）
    """
    with pytest.raises(KeyError):
        slice_source.get("C")


def test_slice_source_requires_symbol_slice_index(tmp_path: Path):
    """
    Contract:
    - manifest 无 index → 明确失败
    """
    meta_dir = tmp_path / "meta"
    meta_dir.mkdir(parents=True)

    stage = "s"
    slot = "o"

    # --------------------------------------------------
    # parquet
    # --------------------------------------------------
    table = pa.table({"x": [1, 2]})
    parquet_path = meta_dir / "t.parquet"
    pq.write_table(table, parquet_path)

    # --------------------------------------------------
    # manifest (注意文件名!!)
    # --------------------------------------------------
    manifest = {
        "outputs": {
            "file": str(parquet_path),
            "rows": 2,
            # ❌ 故意不写 index
        }
    }

    manifest_path = meta_dir / f"{stage}.{slot}.manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    # --------------------------------------------------
    # assertion
    # --------------------------------------------------
    with pytest.raises(RuntimeError):
        SliceSource(
            meta_dir=meta_dir,
            stage=stage,
            output_slot=slot,
        )
