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

# # src/pipeline/parallel/executor.py
# from __future__ import annotations
#
# import os
# from concurrent.futures import ProcessPoolExecutor, as_completed
# from typing import Iterable, Callable, Any
#
# from src.pipeline.parallel.types import ParallelKind
# from src.pipeline.parallel.manifest import ParallelManifest
# from src import logs
#
#
# class ParallelExecutor:
#     """
#     ParallelExecutor（冻结版）
#
#     设计原则：
#     - Step 不创建进程池
#     - Step 不捕获并行异常
#     - 并行安全规则在这里统一 enforce
#     """
#
#     @staticmethod
#     def run(
#         *,
#         kind: ParallelKind,
#         items: Iterable[str],
#         handler: Callable[[str], Any],
#         manifest: ParallelManifest,
#         max_workers: int | None = None,
#         fail_fast: bool = False,
#     ) -> None:
#         items = list(items)
#         if not items:
#             logs.info("[ParallelExecutor] no items to process")
#             return
#
#         # --------------------------------------------------
#         # ① 过滤已完成任务（manifest 驱动）
#         # --------------------------------------------------
#         pending = [
#             item for item in items
#             if not manifest.is_done(item)
#         ]
#
#         if not pending:
#             logs.info("[ParallelExecutor] all items already done")
#             return
#
#         logs.info(
#             f"[ParallelExecutor] start "
#             f"kind={kind} total={len(items)} pending={len(pending)}"
#         )
#
#         # --------------------------------------------------
#         # ② 并发度裁决（统一规则）
#         # --------------------------------------------------
#         workers = ParallelExecutor._resolve_workers(
#             pending,
#             max_workers,
#         )
#
#         # --------------------------------------------------
#         # ③ 执行（唯一允许的并行入口）
#         # --------------------------------------------------
#         if workers == 1:
#             ParallelExecutor._run_sequential(
#                 pending, handler, manifest, fail_fast
#             )
#         else:
#             ParallelExecutor._run_parallel(
#                 pending, handler, manifest, workers, fail_fast
#             )
#
#     # ======================================================
#     # 内部实现
#     # ======================================================
#
#     @staticmethod
#     def _resolve_workers(items: list[str], max_workers: int | None) -> int:
#         cpu = os.cpu_count() or 1
#         if max_workers is None:
#             return min(cpu, len(items))
#         return max(1, min(max_workers, len(items)))
#
#     @staticmethod
#     def _run_sequential(
#         items: list[str],
#         handler: Callable[[str], Any],
#         manifest: ParallelManifest,
#         fail_fast: bool,
#     ) -> None:
#         logs.info("[ParallelExecutor] run sequential")
#
#         for item in items:
#             try:
#                 handler(item)
#                 manifest.mark_done(item)
#             except Exception as e:
#                 manifest.mark_failed(item, str(e))
#                 logs.exception(
#                     f"[ParallelExecutor] failed | item={item}"
#                 )
#
#                 if fail_fast:
#                     raise
#
#     @staticmethod
#     def _run_parallel(
#         items: list[str],
#         handler: Callable[[str], Any],
#         manifest: ParallelManifest,
#         workers: int,
#         fail_fast: bool,
#     ) -> None:
#         logs.info(
#             f"[ParallelExecutor] run parallel | workers={workers}"
#         )
#
#         with ProcessPoolExecutor(max_workers=workers) as pool:
#             futures = {
#                 pool.submit(handler, item): item
#                 for item in items
#             }
#
#             for fut in as_completed(futures):
#                 item = futures[fut]
#                 try:
#                     fut.result()
#                     manifest.mark_done(item)
#                 except Exception as e:
#                     manifest.mark_failed(item, str(e))
#                     logs.exception(
#                         f"[ParallelExecutor] failed | item={item}"
#                     )
#
#                     if fail_fast:
#                         raise
