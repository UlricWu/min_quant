# tests/observability/test_timeline_skip.py
from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.pipeline import DataPipeline
from src.pipeline.context import PipelineContext
from src.pipeline.step import PipelineStep
from src.observability.instrumentation import Instrumentation


class DummySkipStep(PipelineStep):
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


class DummyLeafStep(PipelineStep):
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

    def fact_dir(self, date) -> Path:
        return Path("/tmp/fact")

    def label_dir(self, date) -> Path:
        return Path("/tmp/label")

    def meta_dir(self, date) -> Path:
        return Path("/tmp/meta")

    def feature_dir(self, date) -> Path:
        return Path("/tmp/feature")


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


# ------------------------------------------------------------------
# 核心断言：skip 的 Step 不应写入 timeline
# ------------------------------------------------------------------

def test_skip_step_does_not_pollute_timeline(tmp_path, inst, make_test_pipeline_context):
    pipeline = DataPipeline(
        steps=[DummySkipStep(inst)],
        pm=DummyPathManager(),
        inst=inst,
    )

    ctx = make_test_pipeline_context(tmp_path)

    pipeline.run(date=ctx.today)

    assert inst.timeline == {}, "Skip step should not write anything into timeline"


# ------------------------------------------------------------------
# 对照测试：真实 leaf step 会写入 timeline
# ------------------------------------------------------------------

def test_leaf_step_writes_timeline(inst, tmp_path, make_test_pipeline_context):
    pipeline = DataPipeline(
        steps=[DummyLeafStep(inst)],
        pm=DummyPathManager(),
        inst=inst,
    )
    ctx = make_test_pipeline_context(tmp_path)
    pipeline.run(date=ctx.today)

    assert "DUMMY_LEAF" in inst.timeline
    assert inst.timeline["DUMMY_LEAF"] >= 0.0
