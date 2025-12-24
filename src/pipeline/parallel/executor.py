# src/pipeline/parallel/executor.py
from __future__ import annotations

import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Iterable, Callable, Any

from src.pipeline.parallel.types import ParallelKind
from src import logs


class ParallelExecutor:
    """
    ParallelExecutor（MVP）

    MVP 目标：
    - 提供统一的 ProcessPoolExecutor 封装
    - 不引入持久化状态
    - 不破坏未来 manifest 扩展
    """

    @staticmethod
    def run(
            *,
            kind: ParallelKind,
            items: Iterable[str],
            handler: Callable[[str], Any],
            max_workers: int | None = None,
    ) -> list[Any] | None:
        items = list(items)
        if not items:
            logs.info("[ParallelExecutor] no items to process")
            return

        logs.info(
            f"[ParallelExecutor] start "
            f"kind={kind} total={len(items)}"
        )

        workers = ParallelExecutor._resolve_workers(items, max_workers)

        if workers == 1:
            return ParallelExecutor._run_sequential(items, handler)
        else:
            return ParallelExecutor._run_parallel(items, handler, workers)

    # ---------------- internal ----------------

    @staticmethod
    def _resolve_workers(items: list[str], max_workers: int | None) -> int:
        cpu = os.cpu_count() or 1
        if max_workers is None:
            return min(cpu, len(items))
        return max(1, min(max_workers, len(items)))

    @staticmethod
    def _run_sequential(
            items: list[str],
            handler: Callable[[str], Any],
    ) -> None:
        # logs.info("[ParallelExecutor] run sequential")

        return [handler(item) for item in items]

    @staticmethod
    def _run_parallel(
            items: list[str],
            handler: Callable[[str], Any],
            workers: int,
    ) -> list:
        logs.info(
            f"[ParallelExecutor] run parallel | workers={workers}"
        )

        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(handler, item): item
                for item in items
            }
            results = []
            for fut in as_completed(futures):
                result = fut.result()  # 只取一次
                results.append(result)

        return results  # ← 这是根因