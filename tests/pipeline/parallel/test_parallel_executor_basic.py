# tests/pipeline/parallel/test_parallel_executor_basic.py
from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind


def worker_ok(item: str) -> None:
    # 模拟 CPU 任务
    x = 0
    for _ in range(100_000):
        x += 1


def test_parallel_executor_basic(fake_manifest):
    items = [f"item_{i}" for i in range(4)]

    ParallelExecutor.run(
        kind=ParallelKind.FILE,
        items=items,
        handler=worker_ok,
        manifest=fake_manifest,
        max_workers=2,
    )

    assert fake_manifest.done_items() == set(items)
    assert fake_manifest.failed_items() == set()
