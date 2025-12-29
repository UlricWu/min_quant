from __future__ import annotations

from pathlib import Path

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.normalize_engine import NormalizeEngine
from src.engines.parser_engine import INTERNAL_SCHEMA
from src.meta.meta import BaseMeta, MetaResult
from src.meta.symbol_accessor import SymbolAccessor


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _make_internal_trade_table() -> pa.Table:
    """
    构造一个满足 INTERNAL_SCHEMA 的最小 internal 表，并且包含乱序 (symbol, ts)
    使 NormalizeEngine v2 可以排序 + build index。

    注意：NormalizeEngine v2 只要求 (symbol, ts) 存在且类型正确；
    但 SymbolAccessor / 你的旧契约要求 canonical parquet schema == INTERNAL_SCHEMA，
    因此这里按 INTERNAL_SCHEMA 生成整表。
    """
    schema = INTERNAL_SCHEMA
    n = 4

    # 乱序：故意打乱 symbol/ts，用于验证 Normalize 排序 & index
    symbol_vals = ["600000", "600000", "000001", "300001"]
    ts_vals = [2025120193002000, 2025120193001000, 2025120193003000, 2025120193002000]

    arrays = []
    for field in schema:
        if field.name == "symbol":
            arrays.append(pa.array(symbol_vals, type=field.type))
        elif field.name == "ts":
            arrays.append(pa.array(ts_vals, type=field.type))
        else:
            # 其它列在本测试中不参与计算，填充为 None（按字段类型 cast）
            arrays.append(pa.array([None] * n, type=field.type))

    return pa.Table.from_arrays(arrays, schema=schema)


def _normalize_and_write_parquet(
    *,
    engine: NormalizeEngine,
    table: pa.Table,
    input_file: Path,
    out_dir: Path,
) -> MetaResult:
    """
    模拟 Step 的最小职责：
      - 调 NormalizeEngine(v2) 得到 NormalizeResult (纯内存)
      - 将 canonical 写成 parquet
      - 组装 MetaResult 供 BaseMeta.commit
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    norm = engine.execute([table])
    output_file = out_dir / input_file.name

    # 即使 rows=0，也写一个空 parquet（维持你原来的工程契约）
    pq.write_table(norm.canonical, output_file)

    # MetaResult 的输入/输出/rows/index 仍然是 meta 层的契约
    return MetaResult(
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
    现在 NormalizeEngine v2 不再读取 parquet。
    但 MetaResult / manifest 仍保留 input_file 语义，因此这里保留一个 “输入文件路径实体”。
    """
    p = tmp_path / "sh_trade.parquet"
    p.write_bytes(b"")  # 仅占位：不参与读取
    return p


@pytest.fixture
def internal_trade_table() -> pa.Table:
    return _make_internal_trade_table()


# -----------------------------------------------------------------------------
# Tests
# -----------------------------------------------------------------------------
def test_normalize_and_commit_meta(
    sh_trade_parquet: Path,
    internal_trade_table: pa.Table,
    tmp_path: Path,
):
    engine = NormalizeEngine()

    out_dir = tmp_path / "normalize"
    meta_dir = tmp_path / "meta"

    # ---- Normalize(v2) + Step-like write ----
    result = _normalize_and_write_parquet(
        engine=engine,
        table=internal_trade_table,
        input_file=sh_trade_parquet,
        out_dir=out_dir,
    )

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


def test_symbol_accessor_basic(
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

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    manifest_path = meta.manifest_path(result.output_file.stem)

    accessor = SymbolAccessor.from_manifest(manifest_path)

    assert accessor.rows == result.rows
    assert accessor.schema == INTERNAL_SCHEMA


def test_symbol_accessor_symbols_match_index(
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

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(meta.manifest_path(result.output_file.stem))

    assert set(accessor.symbols()) == set(result.index.keys())


def test_symbol_accessor_slice_correctness(
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

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(meta.manifest_path(result.output_file.stem))

    for symbol in accessor.symbols():
        table = accessor.get(symbol)
        symbols = set(table["symbol"].to_pylist())
        assert symbols == {symbol}


def test_symbol_accessor_slice_size_matches_index(
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

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(meta.manifest_path(result.output_file.stem))

    for symbol, (_start, length) in result.index.items():
        assert accessor.symbol_size(symbol) == length


def test_symbol_accessor_missing_symbol_returns_empty(
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

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(meta.manifest_path(result.output_file.stem))

    empty = accessor.get("999999")
    assert empty.num_rows == 0
    assert empty.schema == accessor.schema


def test_accessor_table_matches_parquet(
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

    meta = BaseMeta(meta_dir, stage="normalize")
    meta.commit(result)

    accessor = SymbolAccessor.from_manifest(meta.manifest_path(result.output_file.stem))

    parquet_table = pq.read_table(result.output_file)
    assert accessor.table.equals(parquet_table)
