from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.steps.feature_build_step import FeatureBuildStep
from src.pipeline.context import PipelineContext
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
            pa.array([1.0] * table.num_rows),
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
            pa.array([2.0] * table.num_rows),
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
            pa.array([999.0] * table.num_rows),
        )


# =============================================================================
# Helpers
# =============================================================================
def write_fact_min(fact_dir: Path) -> Path:
    """
    构造最小 fact/*.min.parquet（2 symbols）
    """
    table = pa.table(
        {
            "symbol": ["AAA", "AAA", "BBB", "BBB"],
            "close": [10.0, 11.0, 20.0, 21.0],
        }
    )

    path = fact_dir / "sh_trade.min.parquet"
    pq.write_table(table, path)
    return path


def write_min_manifest(meta_dir: Path, fact_dir: Path, table: pa.Table) -> None:
    """
    写 min stage manifest（symbol slice index）

    冻结契约：
      - BaseMeta 使用 (stage, output_slot) 决定 manifest 文件名：
          <meta_dir>/<stage>.<output_slot>.manifest.json
      - output_file 必须 exists()
      - index 形态为 {symbol: (start, length)}
    """
    index = {
        "AAA": (0, 2),
        "BBB": (2, 2),
    }

    stage = "min"
    output_slot = "sh_trade"

    output_file = fact_dir / "sh_trade.min.parquet"
    assert output_file.exists(), "fact min parquet must exist before committing meta"

    meta = BaseMeta(
        meta_dir=meta_dir,
        stage=stage,
        output_slot=output_slot,
    )
    meta.commit(
        MetaOutput(
            input_file=fact_dir / "sh_trade.enriched.parquet",  # 占位即可
            output_file=output_file,  # ✅ 必须真实存在
            rows=table.num_rows,
            index=index,
        )
    )

    # 可选：把路径 contract 也在测试里锁死（强烈建议保留）
    manifest_path = meta_dir / f"{stage}.{output_slot}.manifest.json"
    assert manifest_path.exists()



# =============================================================================
# Contract-1 / 2 / 5
#   - symbol-local
#   - 行数不变
#   - L0 → L1 正常 append
# =============================================================================
def test_feature_build_symbol_local_and_row_count(tmp_path: Path, make_test_pipeline_context):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    table = pq.read_table(fact_file)
    write_min_manifest(ctx.meta_dir, ctx.fact_dir, table)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )

    step.run(ctx)

    out = pq.read_table(ctx.feature_dir / "sh_trade.feature.parquet")

    assert out.num_rows == table.num_rows
    assert "l0_dummy" in out.column_names
    assert "l1_dummy" in out.column_names


# =============================================================================
# Contract-3
#   - L1 不能在没有 L0 的情况下运行
# =============================================================================
def test_feature_build_l1_requires_l0(tmp_path: Path, make_test_pipeline_context):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    table = pq.read_table(fact_file)
    write_min_manifest(ctx.meta_dir, ctx.fact_dir, table)

    step = FeatureBuildStep(
        l0_engine=None,
        l1_engines=[DummyL1Engine()],  # ❌
    )

    with pytest.raises(AssertionError):
        step.run(ctx)


# =============================================================================
# Contract-4
#   - append-only
#   - 禁止覆盖 fact 列
# =============================================================================
def test_feature_build_forbids_fact_override(tmp_path: Path, make_test_pipeline_context):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    table = pq.read_table(fact_file)
    write_min_manifest(ctx.meta_dir, ctx.fact_dir, table)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[BadL1EngineOverride()],  # ❌
        only_feature_columns=True,
    )

    step.run(ctx)

    out = pq.read_table(ctx.feature_dir / "sh_trade.feature.parquet")

    # close 必须保持不变
    assert out["close"].to_pylist() == table["close"].to_pylist()


# =============================================================================
# Contract-6
#   - atomic output（单 parquet）
# =============================================================================
def test_feature_build_atomic_output(tmp_path: Path, make_test_pipeline_context):
    ctx = make_test_pipeline_context(tmp_path)

    fact_file = write_fact_min(ctx.fact_dir)
    table = pq.read_table(fact_file)
    write_min_manifest(ctx.meta_dir, ctx.fact_dir, table)

    step = FeatureBuildStep(
        l0_engine=DummyL0Engine(),
        l1_engines=[DummyL1Engine()],
    )

    step.run(ctx)

    outputs = list(ctx.feature_dir.glob("*.parquet"))
    assert len(outputs) == 1
