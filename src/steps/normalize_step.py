#!filepath: src/pipeline/steps/normalize_step.py
from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from src import logs
from src.engines.normalize_engine import NormalizeEngine, NormalizeResult
from src.engines.parser_engine import parse_events_arrow
from src.meta.base import BaseMeta
from src.pipeline.context import PipelineContext
from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind
from src.pipeline.step import PipelineStep

from src.pipeline.context import EngineContext


# -----------------------------------------------------------------------------
# Worker (Process-safe)
# -----------------------------------------------------------------------------
def normalize_one(
        *,
        input_file: Path,
        output_dir: Path,
        batch_size: int,
) -> dict:
    """
    Normalize worker（进程安全）

    约束：
    - top-level function
    - 不依赖 ctx / self
    - 不写 meta（由主进程统一 commit）
    - 不写日志（避免多进程日志乱序）
    """
    engine = NormalizeEngine()

    exchange, kind = input_file.stem.split("_", 1)

    stage = "normalize"
    output_file = output_dir / f'{input_file.stem}.{stage}.parquet'

    pf = pq.ParquetFile(input_file)
    tables: List[pa.Table] = []

    # 1) read + filter + parse (batch level)  —— I/O 属于 Step worker
    for batch in pf.iter_batches(batch_size=batch_size):
        table = pa.Table.from_batches([batch])

        table = engine.filter_a_share_arrow(table)
        if table.num_rows == 0:
            continue

        table = parse_events_arrow(
            table,
            exchange=exchange,
            kind=kind,
        )
        if table.num_rows == 0:
            continue

        tables.append(table)

    # 2) pure compute  —— Engine
    result: NormalizeResult = engine.execute(tables)

    # 3) write parquet —— I/O 属于 Step worker
    pq.write_table(result.canonical, output_file)
    logs.info(f'[NormalizeStep] {input_file.name} normalized ->{output_dir.name} with rows: {result.rows}')

    # 4) return meta payload (pure dict)
    meta = {
        "input_file": input_file,
        "output_file": output_file,
        "rows": result.rows,
        "index": result.index,
    }
    return meta


def _normalize_handler(payload: dict) -> dict:
    return normalize_one(
        input_file=payload["input_file"],
        output_dir=payload["output_dir"],
        batch_size=payload["batch_size"],
    )


class NormalizeStep(PipelineStep):
    """
    NormalizeStep（ProcessPool 并行版 · 冻结）

    并行模型：
      - 主进程：meta 判定 + commit + 日志
      - 子进程：I/O(read+write) + parse + 纯计算（Engine）
    """
    stage = 'normalize'

    def __init__(
            self,
            inst=None,
            *,
            batch_size: int = 50_000_000,
            max_workers: int = 2,
    ):
        super().__init__(inst)
        self.batch_size = batch_size
        self.max_workers = max_workers

    def run(self, ctx: PipelineContext) -> PipelineContext:
        # input_dir = ctx.parquet_dir
        # output_dir = ctx.fact_dir
        #
        # meta = BaseMeta(meta_dir=ctx.meta_dir, stage="normalize")
        #
        # # ① master：筛选需要处理的文件
        inputs: list[Path] = []
        for input_file in ctx.parquet_dir.glob("*.parquet"):
            unit = input_file.stem.split(".", 1)[0].lower()
            meta = BaseMeta(
                meta_dir=ctx.meta_dir,
                stage=self.stage,
                output_slot=unit,
            )
            if not meta.upstream_changed():
                logs.warning(
                    f"[{self.stage}] meta hit → skip {input_file.name}"
                )
                continue
            inputs.append(input_file)

        if not inputs:
            logs.warning(f"[{self.stage}] no files to normalize")
            return ctx

        #
        # ② master：准备并行参数（纯数据）
        items = [
            {
                "input_file": path,
                "output_dir": ctx.fact_dir,
                "batch_size": self.batch_size,
            }
            for path in inputs
        ]
        #
        # # ③ 并行执行（无闭包）
        with self.inst.timer(f"[{self.stage}] parallel normalize | files={len(items)}"):
            results = ParallelExecutor.run(
                kind=ParallelKind.FILE,
                items=items,
                handler=_normalize_handler,
                max_workers=self.max_workers,
            )

        if results is None:
            raise RuntimeError("[{self.stage}] ParallelExecutor returned None")
        # ④ master：统一 commit meta
        for r in results:
            unit = Path(r['input_file']).stem.split(".", 1)[0].lower()
            meta = BaseMeta(
                meta_dir=ctx.meta_dir,
                stage=self.stage,
                output_slot=unit,
            )
            meta.commit(r)  # r is a dict payload

        return ctx
