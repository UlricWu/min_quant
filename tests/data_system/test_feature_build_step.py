from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.data_system.steps.feature_build_step import FeatureBuildStep
from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource
from src.data_system.context import DataContext


# =============================================================================
# Dummy Engines (contract validation only)
# =============================================================================

class DummyL0Engine:
    """
    合法 L0：
    - 不跨时间
    - 只 append l0_* 列
    """

    def execute(self, table: pa.Table) -> pa.Table:
        return table.append_column(
            "l0_dummy",
            pa.array([1.0] * table.num_rows, type=pa.float64()),
        )


class DummyL1Engine:
    """
    合法 L1：
    - 必须依赖 l0_*
    - 只 append l1_* 列
    """

    def execute(self, table: pa.Table) -> pa.Table:
        assert any(c.startswith("l0_") for c in table.column_names)
        return table.append_column(
            "l1_dummy",
            pa.array([2.0] * table.num_rows, type=pa.float64()),
        )


class BadL1EngineOverride:
    """
    非法 L1：
    - 试图覆盖 fact 列（close）
    """

    def execute(self, table: pa.Table) -> pa.Table:
        return table.set_column(
            table.column_names.index("close"),
            "close",
            pa.array([999.0] * table.num_rows, type=pa.float64()),
        )


# =============================================================================
# Helpers
# =============================================================================

def _build_index_python(symbols: list[str]) -> Dict[str, Tuple[int, int]]:
    """
    纯 Python reference：
    假定 symbols 已按 symbol 分块连续
    """
    if not symbols:
        return {}

    index: Dict[str, Tuple[int, int]] = {}
    start = 0
    cur = symbols[0]

    for i in range(1, len(symbols)):
        if symbols[i] != cur:
            index[cur] = (start, i - start)
            start = i
            cur = symbols[i]

    index[cur] = (start, len(symbols) - start)
    return index


def write_fact_min(fact_dir: Path) -> pa.Table:
    """
    构造最小 fact/min.*.parquet（2 symbols + ts）

    冻结点：
    - FeatureBuildStep 会 canonicalize(sort by symbol, ts)
    """
    table = pa.table(
        {
            "symbol": ["AAA", "AAA", "BBB", "BBB"],
            "ts": [2, 1, 2, 1],  # 故意乱序
            "close": [10.0, 11.0, 20.0, 21.0],
        }
    )

    path = fact_dir / "min.sh_trade.parquet"
    pq.write_table(table, path)
    return table


def write_min_manifest(
    *,
    meta_dir: Path,
    fact_dir: Path,
    table: pa.Table,
) -> None:
    """
    写 min stage manifest（symbol slice index）
    """
    index = {
        "AAA": (0, 2),
        "BBB": (2, 2),
    }

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage="min",
        output_slot="sh_trade",
    )

    meta.commit(
        MetaOutput(
            input_file=fact_dir / "dummy_input.parquet",
            output_file=fact_dir / "min.sh_trade.parquet",
            rows=table.num_rows,
            index=index,
        )
    )


# =============================================================================
# Tests
# =============================================================================

def test_feature_build_symbol_local_and_row_count(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )
    step.run(data_ctx)

    out = pq.read_table(data_ctx.feature_dir / "feature.sh_trade.parquet")

    assert out.num_rows == base.num_rows
    assert "l0_dummy" in out.column_names
    assert "l1_dummy" in out.column_names


def test_feature_build_l1_requires_l0(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    step = FeatureBuildStep(
        l0_engine=None,
        l1_engines=[DummyL1Engine()],
    )

    with pytest.raises(AssertionError):
        step.run(data_ctx)


def test_feature_build_ignores_fact_override_when_feature_only(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[BadL1EngineOverride()],
        only_feature_columns=True,
    )
    step.run(data_ctx)

    out = pq.read_table(data_ctx.feature_dir / "feature.sh_trade.parquet")

    base_map = {
        (s, ts): close
        for s, ts, close in zip(
            base["symbol"].to_pylist(),
            base["ts"].to_pylist(),
            base["close"].to_pylist(),
        )
    }

    for s, ts, close in zip(
        out["symbol"].to_pylist(),
        out["ts"].to_pylist(),
        out["close"].to_pylist(),
    ):
        assert base_map[(s, ts)] == close


def test_feature_build_atomic_output(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )
    step.run(data_ctx)

    outputs = list(data_ctx.feature_dir.glob("*.parquet"))
    assert outputs == [data_ctx.feature_dir / "feature.sh_trade.parquet"]


def test_feature_build_preserves_symbol_contiguity(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    ).run(data_ctx)

    out = pq.read_table(data_ctx.feature_dir / "feature.sh_trade.parquet")
    symbols = out["symbol"].to_pylist()

    assert symbols == sorted(symbols)


def test_feature_build_canonicalizes_ts_within_symbol(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    ).run(data_ctx)

    out = pq.read_table(data_ctx.feature_dir / "feature.sh_trade.parquet")
    symbols = out["symbol"].to_pylist()
    ts = out["ts"].to_pylist()

    for i in range(1, len(symbols)):
        if symbols[i] == symbols[i - 1]:
            assert ts[i] >= ts[i - 1]


def test_feature_build_commits_meta_with_index(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    ).run(data_ctx)

    source = SliceSource(
        meta_dir=data_ctx.meta_dir,
        stage="feature",
        output_slot="sh_trade",
    )

    assert set(source.symbols()) == {"AAA", "BBB"}
    assert set(source.get("AAA")["symbol"].to_pylist()) == {"AAA"}
    assert set(source.get("BBB")["symbol"].to_pylist()) == {"BBB"}


def test_feature_build_index_matches_python_reference(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    ).run(data_ctx)

    out = pq.read_table(data_ctx.feature_dir / "feature.sh_trade.parquet")
    symbols = out["symbol"].to_pylist()
    py_index = _build_index_python(symbols)

    source = SliceSource(
        meta_dir=data_ctx.meta_dir,
        stage="feature",
        output_slot="sh_trade",
    )

    for sym, (_, length) in py_index.items():
        assert source.get(sym).num_rows == length


def test_feature_build_meta_hit_skips_second_run(data_ctx: DataContext):
    base = write_fact_min(data_ctx.fact_dir)
    write_min_manifest(
        meta_dir=data_ctx.meta_dir,
        fact_dir=data_ctx.fact_dir,
        table=base,
    )

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )

    step.run(data_ctx)
    out_path = data_ctx.feature_dir / "feature.sh_trade.parquet"
    mtime1 = out_path.stat().st_mtime_ns

    step.run(data_ctx)
    mtime2 = out_path.stat().st_mtime_ns

    assert mtime1 == mtime2
