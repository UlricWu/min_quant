#!filepath: src/steps/label_build_step.py
from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.meta.meta import BaseMeta, MetaResult
from src.meta.symbol_accessor import SymbolAccessor
from src.engines.labels.base import BaseLabelEngine
from src.utils.logger import logs
from src.meta.symbol_slice_source import SymbolSliceSource


def _exchange_from_min_filename(input_file: Path) -> str:
    """
    与 FeatureBuildStep 保持一致：
      sh_trade.min.parquet -> sh_trade
      sz_trade.min.parquet -> sz_trade
    """
    name = input_file.name
    if name.endswith(".parquet"):
        name = name[: -len(".parquet")]
    if name.endswith(".min"):
        name = name[: -len(".min")]
    if name.endswith(".trade_min"):
        name = name[: -len(".trade_min")]
    return name


def _stem_for_manifest(input_file: Path) -> str:
    """
    Frozen rule:
      upstream manifest stem == exchange key
    """
    return _exchange_from_min_filename(input_file)


# -----------------------------------------------------------------------------
# LabelBuildStep (FINAL / FROZEN)
# -----------------------------------------------------------------------------
class LabelBuildStep(PipelineStep):
    """
    LabelBuildStep（FINAL / FROZEN）

    Semantics:
      fact/<exchange>.min.parquet
        -> label/<exchange>.label.parquet

    Principles (frozen):
      - Label 是派生数据资产（不反推事实）
      - Symbol slicing 100% 复用 min-stage manifest
      - Engine 只做 row-based 纯计算
      - 唯一允许的循环：per-symbol
      - 不 group、不整表 shift
      - 单 parquet 写入 + 单次 Meta commit
    """

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

        # stage for this step
        stage = "label"
        meta = BaseMeta(meta_dir, stage=stage)

        # upstream stage (symbol slice index lives here)
        upstream_stage = "min"
        meta_up = BaseMeta(meta_dir, stage=upstream_stage)

        # ------------------------------------------------------------------
        # Iterate fact inputs (single upstream)
        # ------------------------------------------------------------------
        for input_file in sorted(fact_dir.glob("*.min.parquet")):
            exchange = _exchange_from_min_filename(input_file)

            # --------------------------------------------------------------
            # Upstream change check
            # --------------------------------------------------------------
            if not meta.upstream_changed(input_file):
                logs.warning(f"[Label] {exchange} unchanged -> skip")
                continue

            # --------------------------------------------------------------
            # Load fact table
            # --------------------------------------------------------------
            table = pq.read_table(input_file)
            if table.num_rows == 0:
                logs.warning(f"[Label] {exchange} empty input -> skip")
                continue

            # --------------------------------------------------------------
            # Resolve manifest -> SymbolAccessor
            # --------------------------------------------------------------

            source = SymbolSliceSource(
                meta=meta_up,
                input_file=input_file,
                stage='min',
            )

            # --------------------------------------------------------------
            # Per-symbol label computation
            # --------------------------------------------------------------
            label_tables: List[pa.Table] = []

            with self.inst.timer(f"LabelBuild_{exchange}"):
                symbol_count = 0
                for symbol, sub in source.bind(table):
                    if sub.num_rows == 0:
                        continue

                    out = self.engine.execute(sub)
                    label_tables.append(out)
                    symbol_count += 1

                if not label_tables:
                    logs.warning(f"[Label] {exchange} no symbols produced")
                    continue

                # ----------------------------------------------------------
                # Concatenate (symbol blocks already aligned)
                # ----------------------------------------------------------
                result = pa.concat_tables(label_tables, promote_options="default")

                output_file = label_dir / f"{exchange}.{stage}.parquet"
                pq.write_table(result, output_file)

                # ----------------------------------------------------------
                # Commit Meta
                # ----------------------------------------------------------
                meta.commit(
                    MetaResult(
                        input_file=input_file,
                        output_file=output_file,
                        rows=result.num_rows,
                        # label stage usually doesn't need index
                    )
                )

                logs.info(
                    f"[Label] written {output_file.name} "
                    f"symbols={symbol_count} "
                    f"(rows={result.num_rows}, cols={len(result.column_names)})"
                )

        return ctx
