from __future__ import annotations

import os
import time
import pytest
from multiprocessing import Manager

from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind


def parallel_test_handler(args):
    x, called = args
    called.append(x)


def test_run_with_empty_items_does_nothing():
    called = []

    def handler(x):
        called.append(x)

    ParallelExecutor.run(
        kind=ParallelKind.FILE,
        items=[],
        handler=handler,
    )

    assert called == []


def test_run_sequential_order_preserved():
    called = []

    def handler(x):
        called.append(x)

    items = ["a", "b", "c"]

    ParallelExecutor.run(
        kind=ParallelKind.FILE,
        items=items,
        handler=handler,
        max_workers=1,
    )

    assert called == items


# def test_run_parallel_all_items_processed():
#     manager = Manager()
#     called = manager.list()
#
#     items = ["a", "b", "c", "d"]
#
#     ParallelExecutor.run(
#         kind=ParallelKind.FILE,
#         items=[(x, called) for x in items],
#         handler=parallel_test_handler,
#         max_workers=2,
#     )
#
#     assert sorted(called) == sorted(items)



def handler(x):
    if x == "bad":
        raise RuntimeError("boom")


# def test_run_parallel_propagates_exception():
#     items = ["ok1", "bad", "ok2"]
#
#     with pytest.raises(RuntimeError):
#         ParallelExecutor.run(
#             kind=ParallelKind.FILE,
#             items=items,
#             handler=handler,
#             max_workers=2,
#         )


def test_resolve_workers_caps_by_items():
    items = ["a", "b"]
    workers = ParallelExecutor._resolve_workers(items, max_workers=10)
    assert workers == 2


def test_resolve_workers_caps_by_cpu():
    items = list(range(100))
    cpu = os.cpu_count() or 1

    workers = ParallelExecutor._resolve_workers(items, max_workers=None)
    assert workers <= cpu


def test_resolve_workers_at_least_one():
    items = ["a"]
    workers = ParallelExecutor._resolve_workers(items, max_workers=0)
    assert workers == 1
