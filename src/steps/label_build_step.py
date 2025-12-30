# src/steps/label_build_step.py
from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource
from src.engines.labels.base import BaseLabelEngine
from src.utils.logger import logs


# -----------------------------------------------------------------------------
# LabelBuildStep (FINAL / FROZEN)
# -----------------------------------------------------------------------------
class LabelBuildStep(PipelineStep):
    """
    LabelBuildStep（FINAL / FROZEN）

    输入：
      fact/*.min.parquet        （multi-symbol, minute-level）

    输出：
      label/*.label.parquet

    冻结原则：
      - orchestration only
      - label 是派生数据资产
      - engine 只处理单 slice
      - slice discovery 完全由 SliceSource 驱动
      - 不 bind、不 read table
      - 单 parquet 写入 + 单次 Meta commit
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

        for input_file in sorted(fact_dir.glob(f"*.{self.upstream_stage}.parquet")):
            name = input_file.stem.split(".")[0]

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
            # 3. per-slice label computation
            # --------------------------------------------------
            with self.inst.timer(f"[{self.stage}] {name}"):
                slice_count = 0

                for symbol, sub in source:
                    if sub.num_rows == 0:
                        continue

                    out = self.engine.execute(sub)
                    if out.num_rows == 0:
                        continue

                    label_tables.append(out)
                    slice_count += 1

            if not label_tables:
                logs.warning(f"[{self.stage}] {name} no labels produced")
                continue

            # --------------------------------------------------
            # 4. concat + write
            # --------------------------------------------------
            result = pa.concat_tables(label_tables, promote_options="default")

            output_file = label_dir / f"{name}.{self.stage}.parquet"
            pq.write_table(result, output_file)

            # --------------------------------------------------
            # 5. commit meta
            # --------------------------------------------------
            meta.commit(
                MetaOutput(
                    input_file=input_file,
                    output_file=output_file,
                    rows=result.num_rows,
                    # label stage 通常不声明 slice capability
                )
            )

            logs.info(
                f"[{self.stage}] written {output_file.name} "
                f"slices={slice_count} "
                f"(rows={result.num_rows}, cols={len(result.column_names)})"
            )

        return ctx
