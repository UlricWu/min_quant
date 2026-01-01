from __future__ import annotations

from pathlib import Path
from typing import List, Dict

import pyarrow as pa

from src import logs
from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind

from src.meta.base import BaseMeta, MetaOutput

from src.engines.parser_engine import parse_events_arrow

from src.utils.csv7z_batch_source import Csv7zBatchSource
from src.engines.normalize_engine import NormalizeEngine
from src.utils.parquet_writer import ParquetAppendWriter
from src.engines.symbol_index_engine import SymbolIndexEngine
from src.engines.raw_unit_builder import RawUnitBuilder


# =============================================================================
# Worker (process-safe)
# =============================================================================
def fact_build_one(
        *,
        input_file: Path,
        output_file: Path,
        batch_size: int,
        exchange: str,
        kind: str
) -> Dict:
    """
    FactBuild worker（冻结版 / 进程安全）

    约束（冻结）：
    - top-level function
    - 不依赖 ctx / self
    - 不写 meta
    - 不做 orchestration
    - 失败直接抛异常（由 master 决策）

    语义：
    raw/*.csv.7z
        → ConvertEngine（CSV → Arrow）
        → parse_events_arrow
        → NormalizeEngine
        → fact/*.normalize.parquet
    """

    normalize_engine = NormalizeEngine()
    writer = ParquetAppendWriter(output_file=output_file)

    tables: List[pa.Table] = []

    for record_batch in Csv7zBatchSource(input_file):
        # 0) RecordBatch → Table（单 batch）
        table = pa.Table.from_batches([record_batch])

        # 1) parse（单 batch）
        table = parse_events_arrow(table, kind=kind, exchange=exchange)
        if table is None or table.num_rows == 0:
            continue

        # 2) normalize（单 batch）
        table = normalize_engine.execute(table)
        if table is None or table.num_rows == 0:
            continue

        tables.append(table)

    big_table = pa.concat_tables(tables)

    sorted_table, index = SymbolIndexEngine.execute(big_table)

    writer.write(sorted_table, max_rows_per_chunk=batch_size)

    writer.close()

    # --------------------------------------------------
    # 6. 返回 meta payload（纯 dict）
    # --------------------------------------------------
    return {
        "input_file": input_file,
        "output_file": output_file,
        "rows": writer.rows,
        "index": index,
        "output_slot": '_'.join([exchange, kind])
    }


def _fact_build_handler(payload: Dict) -> Dict:
    return fact_build_one(
        input_file=payload["input_file"],
        output_file=payload["output_dir"],
        batch_size=payload["batch_size"],
        exchange=payload['exchange'],
        kind=payload['kind']
    )


# =============================================================================
# Step
# =============================================================================
class ConvertStep(PipelineStep):
    """
    FactBuildStep（FINAL / FROZEN）

    语义：
      raw source
        → (streaming batch source)
        → ParseEngine (single batch)
        → NormalizeEngine (single batch)
        → ParquetAppendWriter (append)
        → [尾部] 全量排序 + overwrite parquet + build symbol index + meta commit

    冻结原则：
    1. Convert 不形成中间 stage
    2. meta 仅记录最终 fact
    3. worker 负责 I/O + compute
    4. master 只负责：
       - meta 判定
       - 并行调度
       - meta commit
    """

    stage = "convert"
    upstream_stage = "raw"

    def __init__(
            self,
            inst=None,
            batch_size: int = 20_000_000,
            max_workers: int = 2,
    ):
        super().__init__(inst)
        self.batch_size = batch_size
        self.max_workers = max_workers

    # ------------------------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:

        # --------------------------------------------------
        # 1. meta-first：判定哪些 raw 文件需要处理
        # --------------------------------------------------
        builder = RawUnitBuilder()
        raw_units = {}

        for input_file in ctx.raw_dir.glob("*.7z"):
            raw_units.update(builder.build(input_file))

        inputs = {}

        for key, input_file in raw_units.items():
            meta = BaseMeta(
                meta_dir=ctx.meta_dir,
                stage=self.stage,
                output_slot=key,  # ← 关键：sh_order
            )

            if not meta.upstream_changed():
                logs.warning(
                    f"[{self.stage}] meta hit → skip {input_file.name}"
                )
                continue

            inputs[key] = input_file

        if not inputs:
            logs.info(f"[{self.stage}] no raw files to process")
            return ctx

        # --------------------------------------------------
        # 2. 构造并行 payload（纯数据）
        # --------------------------------------------------
        items = [
            {
                "input_file": input_path,
                "output_dir": ctx.fact_dir / f'{self.stage}.{key}.parquet',
                "batch_size": self.batch_size,
                "exchange": key.split("_")[0],
                "kind": key.split("_")[1]
            }
            for key, input_path in inputs.items()
        ]
        #
        # # --------------------------------------------------
        # # 3. 并行执行
        # # --------------------------------------------------
        with self.inst.timer(
                f"[{self.stage}] fact build | files={len(items)}"
        ):
            results = ParallelExecutor.run(
                kind=ParallelKind.FILE,
                items=items,
                handler=_fact_build_handler,
                max_workers=self.max_workers,
            )

        if results is None:
            raise RuntimeError(f"[{self.stage}] ParallelExecutor returned None")

        # --------------------------------------------------
        # 4. 严格串行 commit meta
        # --------------------------------------------------
        for r in results:
            meta = BaseMeta(
                meta_dir=ctx.meta_dir,
                stage=self.stage,
                output_slot=r['output_slot'],
            )

            meta.commit(
                MetaOutput(
                    input_file=r["input_file"],
                    output_file=r["output_file"],
                    rows=r["rows"],
                    index=r["index"],
                )
            )

            logs.info(
                f"[{self.stage}] committed {Path(r['output_file']).name} "
                f"rows={r['rows']}"
            )

        return ctx
