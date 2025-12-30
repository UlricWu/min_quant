from __future__ import annotations

from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.normalize_engine import NormalizeEngine
from src.engines.parser_engine import INTERNAL_SCHEMA
from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _make_internal_trade_table() -> pa.Table:
    """
    构造一个满足 INTERNAL_SCHEMA 的最小 internal 表，并且包含乱序 (symbol, ts)
    用于验证 NormalizeEngine(v2) 排序 + symbol_slice index。
    """
    schema = INTERNAL_SCHEMA
    n = 4

    symbol_vals = ["600000", "600000", "000001", "300001"]
    ts_vals = [
        2025120193002000,
        2025120193001000,
        2025120193003000,
        2025120193002000,
    ]

    arrays = []
    for field in schema:
        if field.name == "symbol":
            arrays.append(pa.array(symbol_vals, type=field.type))
        elif field.name == "ts":
            arrays.append(pa.array(ts_vals, type=field.type))
        else:
            arrays.append(pa.array([None] * n, type=field.type))

    return pa.Table.from_arrays(arrays, schema=schema)


def _normalize_and_write_parquet(
    *,
    engine: NormalizeEngine,
    table: pa.Table,
    input_file: Path,
    out_dir: Path,
) -> MetaOutput:
    """
    Step-like 最小职责：
      - 调 NormalizeEngine(v2)
      - 写 canonical parquet
      - 返回 MetaOutput
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    norm = engine.execute([table])
    output_file = out_dir / "canonical.parquet"

    pq.write_table(norm.canonical, output_file)

    return MetaOutput(
        input_file=input_file,
        output_file=output_file,
        rows=norm.rows,
        index=norm.index,
    )


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------
@pytest.fixture
def sh_trade_parquet(tmp_path: Path) -> Path:
    """
    占位 input_file（NormalizeEngine v2 不读取 parquet）
    """
    p = tmp_path / "sh_trade.parquet"
    p.write_bytes(b"")
    return p


@pytest.fixture
def internal_trade_table() -> pa.Table:
    return _make_internal_trade_table()


# -----------------------------------------------------------------------------
# Tests (SliceSource-based)
# -----------------------------------------------------------------------------
def test_normalize_and_commit_meta(
    sh_trade_parquet: Path,
    internal_trade_table: pa.Table,
    tmp_path: Path,
):
    engine = NormalizeEngine()

    out_dir = tmp_path / "normalize"
    meta_dir = tmp_path / "meta"

    result = _normalize_and_write_parquet(
        engine=engine,
        table=internal_trade_table,
        input_file=sh_trade_parquet,
        out_dir=out_dir,
    )

    stage = "normalize"
    output_slot = "canonical"

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )
    meta.commit(result)

    manifest_path = meta_dir / f"{stage}.{output_slot}.manifest.json"
    assert manifest_path.exists()


def test_slice_source_basic(
    sh_trade_parquet: Path,
    internal_trade_table: pa.Table,
    tmp_path: Path,
):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    meta_dir = tmp_path / "meta"

    result = _normalize_and_write_parquet(
        engine=engine,
        table=internal_trade_table,
        input_file=sh_trade_parquet,
        out_dir=out_dir,
    )

    stage = "normalize"
    output_slot = "canonical"

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )
    meta.commit(result)

    source = SliceSource(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )

    assert set(source.symbols()) == set(result.index.keys())


def test_slice_source_get_correct_symbol(
    sh_trade_parquet: Path,
    internal_trade_table: pa.Table,
    tmp_path: Path,
):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    meta_dir = tmp_path / "meta"

    result = _normalize_and_write_parquet(
        engine=engine,
        table=internal_trade_table,
        input_file=sh_trade_parquet,
        out_dir=out_dir,
    )

    stage = "normalize"
    output_slot = "canonical"

    BaseMeta(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    ).commit(result)

    source = SliceSource(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )

    for symbol in source.symbols():
        table = source.get(symbol)
        assert set(table["symbol"].to_pylist()) == {symbol}


def test_slice_source_slice_size_matches_index(
    sh_trade_parquet: Path,
    internal_trade_table: pa.Table,
    tmp_path: Path,
):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    meta_dir = tmp_path / "meta"

    result = _normalize_and_write_parquet(
        engine=engine,
        table=internal_trade_table,
        input_file=sh_trade_parquet,
        out_dir=out_dir,
    )

    stage = "normalize"
    output_slot = "canonical"

    BaseMeta(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    ).commit(result)

    source = SliceSource(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )

    for symbol, (_start, length) in result.index.items():
        assert source.get(symbol).num_rows == length


def test_slice_source_missing_symbol_returns_empty(
    sh_trade_parquet: Path,
    internal_trade_table: pa.Table,
    tmp_path: Path,
):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    meta_dir = tmp_path / "meta"

    result = _normalize_and_write_parquet(
        engine=engine,
        table=internal_trade_table,
        input_file=sh_trade_parquet,
        out_dir=out_dir,
    )

    stage = "normalize"
    output_slot = "canonical"

    BaseMeta(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    ).commit(result)

    source = SliceSource(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )

    with pytest.raises(KeyError):
        source.get("999999")


def test_slice_source_table_matches_parquet(
    sh_trade_parquet: Path,
    internal_trade_table: pa.Table,
    tmp_path: Path,
):
    engine = NormalizeEngine()
    out_dir = tmp_path / "normalize"
    meta_dir = tmp_path / "meta"

    result = _normalize_and_write_parquet(
        engine=engine,
        table=internal_trade_table,
        input_file=sh_trade_parquet,
        out_dir=out_dir,
    )

    stage = "normalize"
    output_slot = "canonical"

    BaseMeta(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    ).commit(result)

    source = SliceSource(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )

    parquet_table = pq.read_table(result.output_file)

    reconstructed = pa.concat_tables(
        [source.get(sym) for sym in source.symbols()]
    )

    assert reconstructed.equals(parquet_table)
