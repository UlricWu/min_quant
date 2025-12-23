# tests/pipeline/parallel/test_parallel_executor_manifest.py
from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind

def worker_ok(item: str) -> None:
    # 模拟 CPU-bound 工作
    x = 0
    for _ in range(100_000):
        x += 1

def worker_record(item: str, recorder: list[str]):
    recorder.append(item)


def test_parallel_executor_skip_done(fake_manifest):
    items = ["a", "b", "c"]

    fake_manifest.mark_done("b")

    executed: list[str] = []

    def handler(item: str):
        executed.append(item)

    ParallelExecutor.run(
        kind=ParallelKind.FILE,
        items=items,
        handler=handler,
        manifest=fake_manifest,
        max_workers=1,   # 顺序，便于断言
    )

    assert set(executed) == {"a", "c"}
    assert fake_manifest.done_items() == {"a", "b", "c"}

def test_parallel_executor_sequential_mode(fake_manifest):
    items = ["x", "y"]

    ParallelExecutor.run(
        kind=ParallelKind.FILE,
        items=items,
        handler=worker_ok,
        manifest=fake_manifest,
        max_workers=1,
    )

    assert fake_manifest.done_items() == {"x", "y"}
