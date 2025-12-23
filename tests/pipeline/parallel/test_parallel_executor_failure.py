# tests/pipeline/parallel/test_parallel_executor_failure.py
import pytest

from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind


def worker_maybe_fail(item: str) -> None:
    if item == "bad":
        raise RuntimeError("boom")


def test_parallel_executor_partial_failure(fake_manifest):
    items = ["ok1", "bad", "ok2"]

    ParallelExecutor.run(
        kind=ParallelKind.SYMBOL,
        items=items,
        handler=worker_maybe_fail,
        manifest=fake_manifest,
        max_workers=2,
        fail_fast=False,
    )

    assert fake_manifest.done_items() == {"ok1", "ok2"}
    assert fake_manifest.failed_items() == {"bad"}
def test_parallel_executor_fail_fast(fake_manifest):
    items = ["ok", "bad", "never"]

    with pytest.raises(RuntimeError):
        ParallelExecutor.run(
            kind=ParallelKind.SYMBOL,
            items=items,
            handler=worker_maybe_fail,
            manifest=fake_manifest,
            max_workers=2,
            fail_fast=True,
        )

    # 至少 bad 已经被记录
    assert "bad" in fake_manifest.failed_items()
