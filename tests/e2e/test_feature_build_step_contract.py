from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.data_system.steps.feature_build_step import FeatureBuildStep
from src.meta.base import BaseMeta, MetaOutput


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
    纯 Python reference: 假定 symbols 已按 symbol 分块连续
    返回 {symbol: (start, length)}
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


def write_fact_min(fact_dir: Path) -> Path:
    """
    构造最小 fact/min.*.parquet（2 symbols，且包含 ts）

    关键冻结点：
      - FeatureBuildStep 最终会调用 SymbolIndexEngine(sort by symbol, ts)
      - 因此 upstream(min) 必须提供 ts（否则 feature 阶段无法 canonicalize）
    """
    # 注意：这里刻意制造乱序 ts，以验证 feature 阶段的 canonicalize(sort) 生效
    table = pa.table(
        {
            "symbol": ["AAA", "AAA", "BBB", "BBB"],
            "ts": [2, 1, 2, 1],  # 每个 symbol 内乱序
            "close": [10.0, 11.0, 20.0, 21.0],
        }
    )

    path = fact_dir / "min.sh_trade.parquet"
    pq.write_table(table, path)
    return path


def write_min_manifest(meta_dir: Path, fact_dir: Path, table: pa.Table) -> None:
    """
    写 min stage manifest（symbol slice index）

    冻结契约：
      - BaseMeta 使用 (stage, output_slot) 决定 manifest 文件名：
          <meta_dir>/<stage>.<output_slot>.manifest.json
      - SliceSource 依赖 index（symbol -> (start, length)）
    """
    # 这里的 index 只服务 SliceSource，用于 per-symbol 读取；
    # 不要求 ts 有序，但要求 symbol 分块连续。
    assert table["symbol"].to_pylist() == ["AAA", "AAA", "BBB", "BBB"]

    index = {
        "AAA": (0, 2),
        "BBB": (2, 2),
    }

    stage = "min"
    output_slot = "sh_trade"

    output_file = fact_dir / "min.sh_trade.parquet"
    assert output_file.exists(), "fact min parquet must exist before committing meta"

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )
    meta.commit(
        MetaOutput(
            input_file=fact_dir / "enriched.sh_trade.parquet",  # 占位即可
            output_file=output_file,
            rows=table.num_rows,
            index=index,
        )
    )

    manifest_path = meta_dir / f"{stage}.{output_slot}.manifest.json"
    assert manifest_path.exists()


# =============================================================================
# Tests
# =============================================================================
def test_feature_build_symbol_local_and_row_count(
    tmp_path: Path, make_test_pipeline_context
):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )
    step.run(ctx)

    # ✅ output 命名：feature.{name}.parquet
    out = pq.read_table(ctx.feature_dir / "feature.sh_trade.parquet")

    assert out.num_rows == base.num_rows
    assert "l0_dummy" in out.column_names
    assert "l1_dummy" in out.column_names


def test_feature_build_l1_requires_l0(tmp_path: Path, make_test_pipeline_context):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=None,
        l1_engines=[DummyL1Engine()],  # ❌
    )

    with pytest.raises(AssertionError):
        step.run(ctx)

def test_feature_build_ignores_fact_override_when_feature_only(
    tmp_path: Path, make_test_pipeline_context
):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[BadL1EngineOverride()],
        only_feature_columns=True,
    )
    step.run(ctx)

    out = pq.read_table(ctx.feature_dir / "feature.sh_trade.parquet")

    # --------------------------------------------------
    # 冻结语义：
    #   - 行顺序允许变化（canonicalize）
    #   - (symbol, ts) 对应的 close 值必须保持不变
    # --------------------------------------------------
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


def test_feature_build_atomic_output(tmp_path: Path, make_test_pipeline_context):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )
    step.run(ctx)

    outputs = list(ctx.feature_dir.glob("*.parquet"))
    assert len(outputs) == 1
    assert outputs[0].name == "feature.sh_trade.parquet"


def test_feature_build_preserves_symbol_contiguity(
    tmp_path: Path, make_test_pipeline_context
):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )
    step.run(ctx)

    out = pq.read_table(ctx.feature_dir / "feature.sh_trade.parquet")
    symbols = out["symbol"].to_pylist()

    # 必须是 AAA...AAA BBB...BBB（slice 连续）
    assert symbols == sorted(symbols)


def test_feature_build_canonicalizes_ts_within_symbol(
    tmp_path: Path, make_test_pipeline_context
):
    """
    强冻结：Feature 阶段会做全量 canonicalize(sort by symbol, ts)
    因此同一 symbol 内 ts 必须升序。
    """
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )
    step.run(ctx)

    out = pq.read_table(ctx.feature_dir / "feature.sh_trade.parquet")
    symbols = out["symbol"].to_pylist()
    ts = out["ts"].to_pylist()

    # 分段检查：同 symbol 内 ts 单调不降
    for i in range(1, len(symbols)):
        if symbols[i] == symbols[i - 1]:
            assert ts[i] >= ts[i - 1]


def test_feature_build_commits_meta_with_index(
    tmp_path: Path, make_test_pipeline_context
):
    """
    强冻结：每个 step 都必须 commit index
    """
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )
    step.run(ctx)

    # meta 文件必须存在
    manifest_path = ctx.train_dir / "feature.sh_trade.manifest.json"
    assert manifest_path.exists()

    # 通过 BaseMeta 读取 manifest（如果你 BaseMeta 有 load API 可用就用；没有就只验证 SliceSource）
    # 最稳妥：用 SliceSource 验证 index 可用且 slice 正确
    from src.meta.slice_source import SliceSource

    source = SliceSource(
        meta_dir=ctx.train_dir,
        stage="feature",
        output_slot="sh_trade",
    )

    # 必须可枚举 symbol
    assert set(source.symbols()) == {"AAA", "BBB"}

    # slice 必须正确
    assert set(source.get("AAA")["symbol"].to_pylist()) == {"AAA"}
    assert set(source.get("BBB")["symbol"].to_pylist()) == {"BBB"}


def test_feature_build_index_matches_python_reference(
    tmp_path: Path, make_test_pipeline_context
):
    """
    index 语义冻结：symbol -> (start, length) 覆盖全量且与连续分块一致
    这里不直接读 manifest json，改用 SliceSource 的重构一致性来验证（更强）。
    """
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )
    step.run(ctx)

    out = pq.read_table(ctx.feature_dir / "feature.sh_trade.parquet")
    symbols = out["symbol"].to_pylist()
    py_index = _build_index_python(symbols)

    from src.meta.slice_source import SliceSource

    source = SliceSource(
        meta_dir=ctx.train_dir,
        stage="feature",
        output_slot="sh_trade",
    )

    # 用 SliceSource 的 slice 长度对齐 python index（不依赖 BaseMeta 的私有实现）
    for sym, (_start, length) in py_index.items():
        assert source.get(sym).num_rows == length


def test_feature_build_meta_hit_skips_second_run(
    tmp_path: Path, make_test_pipeline_context
):
    """
    冻结：meta-first；第二次 run 必须跳过（不重写输出）
    """
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    base = pq.read_table(fact_file)
    write_min_manifest(ctx.train_dir, ctx.fact_dir, base)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )

    step.run(ctx)
    out_path = ctx.feature_dir / "feature.sh_trade.parquet"
    assert out_path.exists()
    mtime1 = out_path.stat().st_mtime_ns

    # second run should meta-hit skip
    step.run(ctx)
    mtime2 = out_path.stat().st_mtime_ns

    assert mtime2 == mtime1
