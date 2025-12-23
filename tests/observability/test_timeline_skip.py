# tests/observability/test_timeline_skip.py
from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.pipeline import DataPipeline
from src.pipeline.context import PipelineContext
from src.pipeline.step import BasePipelineStep
from src.observability.instrumentation import Instrumentation


class DummySkipStep(BasePipelineStep):
    """
    一个始终 skip 的 Step：
    - 不产生任何 leaf timer
    - 只用于验证 timeline 不会被污染
    """

    def run(self, ctx: PipelineContext) -> PipelineContext:
        # Step scope（record=False），但内部没有任何 leaf timer
        with self.timed():
            pass
        return ctx


class DummyLeafStep(BasePipelineStep):
    """
    一个会真实执行 leaf timer 的 Step，用于对照测试。
    """

    def run(self, ctx: PipelineContext) -> PipelineContext:
        with self.timed():
            with self.inst.timer("DUMMY_LEAF"):
                pass
        return ctx


class DummyPathManager:
    """最小可用 PathManager stub。"""

    def raw_dir(self, date: str) -> Path:
        return Path("/tmp/raw")

    def parquet_dir(self, date: str) -> Path:
        return Path("/tmp/parquet")

    def symbol_dir(self, date) -> Path:
        return Path("/tmp/symbol")

    def canonical_dir(self, date) -> Path:
        return Path("/tmp/canonical")

    def meta_dir(self, date) -> Path:
        return Path("/tmp/meta")


class DummyFileSystem:
    """防止真实文件系统副作用。"""

    @staticmethod
    def ensure_dir(path: Path):
        pass


@pytest.fixture
def inst():
    return Instrumentation(enabled=True)


@pytest.fixture(autouse=True)
def patch_filesystem(monkeypatch):
    # 避免真实 mkdir
    from src.utils import filesystem

    monkeypatch.setattr(filesystem.FileSystem, "ensure_dir", DummyFileSystem.ensure_dir)


@pytest.fixture
def ctx(tmp_path: Path) -> PipelineContext:
    return PipelineContext(
        date="2025-11-04",
        raw_dir=tmp_path / "raw",
        parquet_dir=tmp_path / "parquet",
        fact_dir=tmp_path / "symbol",
        canonical_dir=tmp_path / "canonical",
        meta_dir=tmp_path/'meta'
    )


# ------------------------------------------------------------------
# 核心断言：skip 的 Step 不应写入 timeline
# ------------------------------------------------------------------

def test_skip_step_does_not_pollute_timeline(inst, ctx):
    pipeline = DataPipeline(
        steps=[DummySkipStep(inst)],
        pm=DummyPathManager(),
        inst=inst,
    )

    pipeline.run(date=ctx.date)

    assert inst.timeline == {}, "Skip step should not write anything into timeline"


# ------------------------------------------------------------------
# 对照测试：真实 leaf step 会写入 timeline
# ------------------------------------------------------------------

def test_leaf_step_writes_timeline(inst, ctx):
    pipeline = DataPipeline(
        steps=[DummyLeafStep(inst)],
        pm=DummyPathManager(),
        inst=inst,
    )

    pipeline.run(date=ctx.date)

    assert "DUMMY_LEAF" in inst.timeline
    assert inst.timeline["DUMMY_LEAF"] >= 0.0
