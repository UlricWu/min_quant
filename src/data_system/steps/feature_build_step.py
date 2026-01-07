#!filepath: src/pipeline/steps/feature_build_step.py
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence

import pyarrow as pa

from src.pipeline.step import PipelineStep
from src.data_system.context import DataContext
from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource
from src.utils.logger import logs

from src.data_system.engines.symbol_index_engine import SymbolIndexEngine
from src.utils.parquet_writer import ParquetAppendWriter


# -----------------------------------------------------------------------------
# Utility: merge append / replace columns
# -----------------------------------------------------------------------------
def merge_append_replace(
        base: pa.Table,
        delta: pa.Table,
        *,
        only_feature_columns: bool = False,
) -> pa.Table:
    """
    Merge delta columns into base table (append / replace).

    Frozen contract:
      - base.num_rows == delta.num_rows
      - row order aligned
    """
    if base.num_rows != delta.num_rows:
        raise ValueError(
            f"[FeatureBuild] row mismatch: base={base.num_rows}, delta={delta.num_rows}"
        )

    out = base
    for name in delta.column_names:
        if only_feature_columns and not (
                name.startswith("l0_")
                or name.startswith("l1_")
                or name.startswith("l2_")
        ):
            continue

        col = delta[name]
        if name in out.column_names:
            idx = out.column_names.index(name)
            out = out.set_column(idx, name, col)
        else:
            out = out.append_column(name, col)

    return out


# -----------------------------------------------------------------------------
# FeatureBuildStep (FINAL / FROZEN)
# -----------------------------------------------------------------------------
class FeatureBuildStep(PipelineStep):
    """
    FeatureBuildStep（FINAL / FROZEN）

    输入：
      fact/min.*.parquet      （multi-symbol, minute-level）

    输出：
      feature/feature.*.parquet

    冻结原则：
      - orchestration only
      - per-symbol execution only
      - slice discovery 由 SliceSource 驱动
      - engine 纯函数、无副作用
      - feature 阶段统一 canonicalize + slice index
    """

    stage = "feature"
    upstream_stage = "min"

    def __init__(
            self,
            *,
            l0_engine: Optional[object] = None,
            l1_engines: Optional[Sequence[object]] = None,
            l2_engine: Optional[object] = None,
            only_feature_columns: bool = False,
            inst=None,
    ) -> None:
        super().__init__(inst)
        self.l0 = l0_engine
        self.l1s = list(l1_engines) if l1_engines is not None else []
        self.l2 = l2_engine
        self.only_feature_columns = only_feature_columns

    # ------------------------------------------------------------------
    def run(self, ctx: DataContext) -> DataContext:
        fact_dir: Path = ctx.fact_dir
        feature_dir: Path = ctx.feature_dir
        meta_dir: Path = ctx.meta_dir

        feature_dir.mkdir(parents=True, exist_ok=True)

        for input_file in fact_dir.glob(f"{self.upstream_stage}.*.parquet"):
            name = input_file.stem.split(".")[1]
            output_file = feature_dir / f"{self.stage}.{name}.parquet"

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

            feature_tables: List[pa.Table] = []

            # --------------------------------------------------
            # 3. per-symbol feature build（engine 纯计算）
            # --------------------------------------------------
            with self.inst.timer(f"[{self.stage}] {name}"):

                for symbol, sub in source:
                    if sub.num_rows == 0:
                        continue

                    out = sub

                    # L0
                    if self.l0 is not None:
                        delta0 = self.l0.execute(out)
                        out = merge_append_replace(
                            out,
                            delta0,
                            only_feature_columns=self.only_feature_columns,
                        )

                    # L1 chain
                    for eng in self.l1s:
                        delta1 = eng.execute(out)
                        out = merge_append_replace(
                            out,
                            delta1,
                            only_feature_columns=self.only_feature_columns,
                        )

                    # L2
                    if self.l2 is not None:
                        delta2 = self.l2.execute(out)
                        out = merge_append_replace(
                            out,
                            delta2,
                            only_feature_columns=self.only_feature_columns,
                        )

                    feature_tables.append(out)

            if not feature_tables:
                logs.warning(f"[{self.stage}] {name} no features produced")
                continue

            # --------------------------------------------------
            # 4. concat + canonicalize + rebuild slice index
            # --------------------------------------------------
            tables, index = SymbolIndexEngine.execute(
                pa.concat_tables(feature_tables)
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
                    index=index,
                )
            )

            logs.info(
                f"[{self.stage}] written {output_file.name} "
                f"symbols={len(feature_tables)} "
                f"(rows={tables.num_rows}, cols={len(tables.column_names)})"
            )

        return ctx
