from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.normalize_engine import NormalizeEngine
from src.engines.parser_engine import INTERNAL_SCHEMA
from src.meta.meta import BaseMeta
from src.meta.meta import MetaResult
from src.meta.symbol_accessor import SymbolAccessor

@pytest.fixture
def sh_trade_parquet(tmp_path: Path) -> Path:
    table = pa.table(
        {
            "SecurityID": [
                "600000",
                "600000",
                "000001",
                "300001",
            ],
            "TickTime": [
                93002000,
                93001000,
                93003000,
                93002000,
            ],
            "TradeTime": [
                2025120193002000,
                2025120193001000,
                2025120193003000,
                2025120193002000,
            ],
            "TickType": [
                "T", "T", "T", "T"
            ],
            "Price": [
                10.1, 10.0, 9.9, 20.0
            ],
            "Volume": [
                200, 100, 150, 300
            ],
            "Side": [
                "2", "1", "1", "2"
            ],
            "SubSeq": [
                2, 1, 3, 4
            ],
            "BuyNo": [
                20, 10, 30, 40
            ],
            "SellNo": [
                21, 11, 31, 41
            ],
        }
    )

    path = tmp_path / "sh_trade.parquet"
    pq.write_table(table, path)
    return path

def test_normalize_and_commit_meta(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()

    out_dir = tmp_path / "normalize"
    out_dir.mkdir()

    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    # ---- Normalize ----
    result = engine.execute(sh_trade_parquet, out_dir)

    assert result.output_file.exists()
    assert result.rows > 0
    assert isinstance(result.index, dict)

    # ---- Meta ----
    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    manifest_path = meta.manifest_path(result.output_file.stem)
    assert manifest_path.exists()

    manifest = meta.load(result.output_file.stem)
    assert manifest is not None
    assert manifest["stage"] == "normalize"
    assert manifest["outputs"]["rows"] == result.rows
def test_symbol_accessor_basic(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()

    out_dir = tmp_path / "normalize"
    out_dir.mkdir()

    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    # ---- Normalize ----
    result = engine.execute(sh_trade_parquet, out_dir)

    # ---- Meta ----
    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    manifest_path = meta.manifest_path(result.output_file.stem)

    # ---- SymbolAccessor ----
    accessor = SymbolAccessor.from_manifest(manifest_path)

    assert accessor.rows == result.rows
    assert accessor.schema == INTERNAL_SCHEMA
def test_symbol_accessor_symbols_match_index(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    out_dir.mkdir()

    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(
        meta.manifest_path(result.output_file.stem)
    )

    assert set(accessor.symbols()) == set(result.index.keys())
def test_symbol_accessor_slice_correctness(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    out_dir.mkdir()

    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(
        meta.manifest_path(result.output_file.stem)
    )

    for symbol in accessor.symbols():
        table = accessor.get(symbol)
        symbols = set(table["symbol"].to_pylist())
        assert symbols == {symbol}
def test_symbol_accessor_slice_size_matches_index(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    out_dir.mkdir()

    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(
        meta.manifest_path(result.output_file.stem)
    )

    for symbol, (start, length) in result.index.items():
        assert accessor.symbol_size(symbol) == length
def test_symbol_accessor_missing_symbol_returns_empty(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    out_dir.mkdir()

    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(
        meta.manifest_path(result.output_file.stem)
    )

    empty = accessor.get("999999")
    assert empty.num_rows == 0
    assert empty.schema == accessor.schema
def test_accessor_table_matches_parquet(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    out_dir.mkdir()

    meta_dir = tmp_path / "meta"
    meta_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(
        meta.manifest_path(result.output_file.stem)
    )

    parquet_table = pq.read_table(result.output_file)
    assert accessor.table.equals(parquet_table)
