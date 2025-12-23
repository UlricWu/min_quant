#!filepath: src/pipeline/steps/orderbook_rebuild_step.py
from __future__ import annotations

import os
import time
import traceback
from dataclasses import dataclass
from multiprocessing import get_context
from pathlib import Path
from typing import Iterable, Optional, Tuple

from src import logs
from src.pipeline.context import PipelineContext, EngineContext
from src.pipeline.step import BasePipelineStep
from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine


# ============================================================
# Worker payload / result
# ============================================================
@dataclass(frozen=True, slots=True)
class _Job:
    symbol: str
    input_path: Path
    output_path: Path


@dataclass(frozen=True, slots=True)
class _JobResult:
    symbol: str
    ok: bool
    skipped: bool
    elapsed_s: float
    error: Optional[str] = None


def _should_skip(*, input_path: Path, output_path: Path) -> bool:
    """
    Skip if output exists and is newer than input.
    (Safe default for partial reruns.)
    """
    if not output_path.exists():
        return False
    try:
        return output_path.stat().st_mtime >= input_path.stat().st_mtime
    except FileNotFoundError:
        return False


def _rebuild_one(job: _Job) -> _JobResult:
    """
    Process entrypoint (must be top-level for multiprocessing pickling).
    - Creates a new engine per process (safe).
    - Offline mode only: build snapshot parquet.
    """
    t0 = time.perf_counter()
    try:
        if not job.input_path.exists():
            return _JobResult(
                symbol=job.symbol,
                ok=False,
                skipped=False,
                elapsed_s=time.perf_counter() - t0,
                error=f"input not found: {job.input_path}",
            )

        if _should_skip(input_path=job.input_path, output_path=job.output_path):
            return _JobResult(
                symbol=job.symbol,
                ok=True,
                skipped=True,
                elapsed_s=time.perf_counter() - t0,
            )

        job.output_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to tmp and atomically replace to avoid partial files
        tmp_out = job.output_path.with_suffix(job.output_path.suffix + ".tmp")

        engine = OrderBookRebuildEngine(record_events=False)
        engine_ctx = EngineContext(
            mode="offline",
            input_path=job.input_path,
            output_path=tmp_out,
            event=None,
            emit_snapshot=True,
        )
        engine.execute(engine_ctx)

        tmp_out.replace(job.output_path)

        return _JobResult(
            symbol=job.symbol,
            ok=True,
            skipped=False,
            elapsed_s=time.perf_counter() - t0,
        )

    except Exception as e:
        err = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
        return _JobResult(
            symbol=job.symbol,
            ok=False,
            skipped=False,
            elapsed_s=time.perf_counter() - t0,
            error=err,
        )


# ============================================================
# Step
# ============================================================
class OrderBookRebuildStep(BasePipelineStep):
    """
    OrderBookRebuildStep（并行版，offline）

    输入（每个 symbol）:
      {symbol_root}/{symbol}/{date}/order.parquet

    输出（每个 symbol）:
      {symbol_root}/{symbol}/{date}/orderbook.parquet

    特性：
      - 多进程并行（symbol 级）
      - 支持部分重跑（输出存在且新于输入则 skip）
      - 原子写入（tmp -> replace）
      - 失败隔离：收集失败列表，最后统一报错
    """

    def __init__(
            self,
            *,
            # symbol_root: Path,
            max_workers: Optional[int] = None,
            mp_start_method: str = "fork",
            inst=None,
    ) -> None:
        super().__init__(inst)
        # self.symbol_root = symbol_root

        cpu = os.cpu_count() or 8
        # 保守一点：避免把机器打满导致 IO 抖动
        self.max_workers = max(1, min(max_workers or (cpu - 2), cpu))

        # Linux 推荐 fork；若你未来在 macOS / Windows，改为 spawn
        self.mp_start_method = mp_start_method

    # ------------------------------------------------------------
    def _iter_jobs(self, input_dir: Path) -> Iterable[_Job]:
        """
        Enumerate symbol jobs from:
          symbol_root/<symbol>/<date>/order.parquet
        """
        # root = self.symbol_root
        if not input_dir.exists():
            raise FileNotFoundError(f"symbol_root not found: {input_dir}")

        for sym_dir in input_dir.iterdir():
            if not sym_dir.is_dir():
                continue

            symbol = sym_dir.name
            # # day_dir = sym_dir / date
            # if not day_dir.exists():
            #     continue

            inp = sym_dir / "order.parquet"
            if not inp.exists():
                continue

            out = sym_dir / "orderbook.parquet"
            yield _Job(symbol=symbol, input_path=inp, output_path=out)

    # ------------------------------------------------------------
    def run(self, ctx: PipelineContext) -> None:
        # date = ctx.date  # 你现有 PipelineContext 已经在用 ctx.date
        input_dir = ctx.fact_dir
        t0 = time.perf_counter()

        jobs = list(self._iter_jobs(input_dir))
        total = len(jobs)

        logs.info("[OrderBookRebuildStep] start rebuilding order.parquet")
        logs.info("[OrderBookRebuildStep] process count: %d", total)

        if total == 0:
            logs.info("[OrderBookRebuildStep] no jobs found, skip")
            return

        with self.inst.timer("OrderBookRebuildStep"):

            # multiprocessing pool
            mp = get_context(self.mp_start_method)
            ok = 0
            skipped = 0
            failed: list[_JobResult] = []

            # 让 imap_unordered 更平衡：chunk 大一点能降低 IPC 开销
            # 经验值：chunk_size ~ 4~16
            chunk_size = 8 if total >= 2000 else 4

            with mp.Pool(processes=self.max_workers) as pool:
                for r in pool.imap_unordered(_rebuild_one, jobs, chunksize=chunk_size):
                    if r.ok:
                        ok += 1
                        if r.skipped:
                            skipped += 1
                    else:
                        failed.append(r)

                    # 低频进度日志：避免刷屏但能看见推进
                    done = ok + len(failed)
                    if done % 500 == 0 or done == total:
                        logs.info(
                            f"[OrderBookRebuildStep] progress {done}/{total} | ok={ok} | skipped={skipped} | failed={len(failed)}",

                        )

            elapsed = time.perf_counter() - t0
            logs.info(
                f"[OrderBookRebuildStep] done | total={total} | ok=%{ok} | skipped={skipped} | failed={len(failed)} | elapsed={elapsed}",

            )

            if failed:
                # 只打印前 N 个失败，避免日志爆炸
                max_show = 20
                logs.error("[OrderBookRebuildStep] failed samples (showing up to %d):", max_show)
                for r in failed[:max_show]:
                    logs.error(f"[OrderBookRebuildStep] symbol={r.symbol} err={r.error}")

                raise RuntimeError(
                    f"[OrderBookRebuildStep] rebuild failed: {len(failed)}/{total} symbols. "
                    f"Check logs for details."
                )
        return ctx

# #!filepath: src/steps/orderbook_rebuild_step.py
# from __future__ import annotations
#
# from pathlib import Path
# from typing import Optional
#
# import pyarrow as pa
# import pyarrow.parquet as pq
#
# from src.pipeline.step import PipelineStep
# from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
# from src.utils.logger import logs
# from src.pipeline.context import EngineContext
#
# class OrderBookRebuildStep(PipelineStep):
#     """
#     OrderBookRebuildStep（symbol-local, existence-only, Arrow-only, v1）
#
#     Rule:
#       if output exists -> SKIP
#       else -> RUN
#     """
#
#     def __init__(self, engine: OrderBookRebuildEngine, inst=None) -> None:
#         self.engine = engine
#         self.inst = inst
#
#     # --------------------------------------------------
#     def run(self, ctx) -> None:
#         input_dir: Path = ctx.symbol_dir
#         input_file = 'order.parquet'
#         out_name = "orderbook.parquet"
#
#         # 遍历当日 universe（和 SymbolSplit 完全一致）
#
#         count = 0
#
#         with self.inst.timer(self.__class__.__name__):
#             logs.info(f'[OrderBookRebuildStep] start rebuilding {input_file}')
#
#             for sym_dir in sorted(input_dir.iterdir()):
#                 if not sym_dir.is_dir():
#                     continue
#
#                 file = sym_dir / input_file
#                 if not file.exists():
#                     # logs.warning(f'[TradeEnrichStep] file {file} not found')
#                     continue
#                 count += 1
#
#                 output_file = sym_dir / out_name
#
#                 if output_file.exists():
#                     continue
#
#                 ctx_engine = EngineContext(
#                     input_path=file,
#                     output_path=output_file,
#                 )
#
#                 self.engine.execute(ctx_engine)
#
#         logs.info(f'[OrderBookRebuildStep] process count: {count}')
#
#         return ctx
