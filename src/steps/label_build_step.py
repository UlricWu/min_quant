#!filepath: src/pipeline/steps/label_build_step.py
from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow as pa

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource
from src.engines.labels.base import BaseLabelEngine
from src.utils.logger import logs

from src.engines.symbol_index_engine import SymbolIndexEngine
from src.utils.parquet_writer import ParquetAppendWriter


# -----------------------------------------------------------------------------
# LabelBuildStep (FINAL / FROZEN)
# -----------------------------------------------------------------------------
class LabelBuildStep(PipelineStep):
    """
    LabelBuildStep（FINAL / FROZEN）

    输入：
      fact/min.*.parquet        （multi-symbol, minute-level）

    输出：
      label/label.*.parquet

    冻结原则：
      - orchestration only
      - label 是派生数据资产
      - engine 只处理单 symbol
      - slice discovery 完全由 SliceSource 驱动
      - 统一 canonicalize + writer + meta
    """

    stage = "label"
    upstream_stage = "min"

    def __init__(
            self,
            *,
            engine: BaseLabelEngine,
            inst=None,
    ) -> None:
        super().__init__(inst)
        self.engine = engine

    # ------------------------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        fact_dir: Path = ctx.fact_dir
        label_dir: Path = ctx.label_dir
        meta_dir: Path = ctx.meta_dir

        label_dir.mkdir(parents=True, exist_ok=True)

        for input_file in fact_dir.glob(f"{self.upstream_stage}.*.parquet"):
            name = input_file.stem.split(".")[1]
            output_file = label_dir / f"{self.stage}.{name}.parquet"

            meta = BaseMeta(
                meta_dir=meta_dir,
                stage=self.stage,
                output_slot=name,
            )

            # --------------------------------------------------
            # 1. upstream check
            # --------------------------------------------------
            if not meta.upstream_changed():
                logs.warning(f"[{self.stage}] meta hit → skip {input_file.name}")
                continue

            # --------------------------------------------------
            # 2. SliceSource（来自 min stage）
            # --------------------------------------------------
            source = SliceSource(
                meta_dir=meta_dir,
                stage=self.upstream_stage,
                output_slot=name,
            )

            label_tables: List[pa.Table] = []

            # --------------------------------------------------
            # 3. per-symbol label computation（engine 纯计算）
            # --------------------------------------------------
            with self.inst.timer(f"[{self.stage}] {name}"):

                for symbol, sub in source:
                    if sub.num_rows == 0:
                        continue

                    out = self.engine.execute(sub)
                    if out.num_rows == 0:
                        continue

                    label_tables.append(out)

            if not label_tables:
                logs.warning(f"[{self.stage}] {name} no labels produced")
                continue

            # --------------------------------------------------
            # 4. concat + canonicalize + rebuild slice index
            # --------------------------------------------------
            tables, index = SymbolIndexEngine.execute(
                pa.concat_tables(label_tables)
            )

            writer = ParquetAppendWriter(output_file=output_file)
            writer.write(tables)
            writer.close()

            # --------------------------------------------------
            # 5. commit meta
            # --------------------------------------------------
            meta.commit(
                MetaOutput(
                    input_file=input_file,
                    output_file=output_file,
                    rows=tables.num_rows,
                    index=index,  # label 默认可 slice（即使暂时不用）
                )
            )

            logs.info(
                f"[{self.stage}] written {output_file.name} "
                f"symbols={len(label_tables)} "
                f"(rows={tables.num_rows}, cols={len(tables.column_names)})"
            )

        return ctx
