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
    ) -> None:
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
            ParallelExecutor._run_sequential(items, handler)
        else:
            ParallelExecutor._run_parallel(items, handler, workers)

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

        for item in items:
            handler(item)

    @staticmethod
    def _run_parallel(
            items: list[str],
            handler: Callable[[str], Any],
            workers: int,
    ) -> None:
        logs.info(
            f"[ParallelExecutor] run parallel | workers={workers}"
        )

        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(handler, item): item
                for item in items
            }

            for fut in as_completed(futures):
                fut.result()  # 失败直接抛
