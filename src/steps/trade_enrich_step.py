# src/pipeline/steps/trade_enrich_step.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource
from src.utils.logger import logs


class TradeEnrichStep(PipelineStep):
    """
    TradeEnrichStep（FINAL / FROZEN）
    前置条件（由本 Step 保证）：
    - table 按 symbol 分块 concat
    - 每个 symbol 内部顺序已保持

    输入：
      fact/*.normalize.parquet   （multi-symbol, canonical）

    输出：
      fact/*.enriched.parquet    （multi-symbol, enriched）

    冻结原则：
      - orchestration only
      - engine 只处理单 symbol
      - enriched 阶段重新生成 slice index
    """

    stage = "enriched"
    upstream_stage = "normalize"

    def __init__(
            self,
            engine: TradeEnrichEngine,
            inst=None,
    ) -> None:
        super().__init__(inst)
        self.engine = engine

    # ------------------------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        input_dir: Path = ctx.fact_dir
        output_dir: Path = ctx.fact_dir
        meta_dir: Path = ctx.meta_dir

        for input_file in input_dir.glob(f"*{self.upstream_stage}.parquet"):
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
                logs.warning(f"[TradeEnrichStep] meta hit → skip {input_file.name}")
                continue

            # --------------------------------------------------
            # 2. 构造 SliceSource（来自 normalize 的 slice capability）
            # --------------------------------------------------
            source = SliceSource(
                meta_dir=ctx.meta_dir,
                stage="normalize",
                output_slot=name,
            )

            enriched_tables: list[pa.Table] = []

            # --------------------------------------------------
            # 3. per-symbol enrich（engine 纯计算）
            # --------------------------------------------------
            with self.inst.timer(f"[TradeEnrich] {name}"):
                symbol_count = 0

                for symbol, sub in source:
                    if sub.num_rows == 0:
                        continue

                    enriched = self.engine.execute(sub)
                    if enriched.num_rows == 0:
                        continue

                    enriched_tables.append(enriched)
                    symbol_count += 1

            if not enriched_tables:
                logs.warning(f"[TradeEnrichStep] {name} no enriched data")
                continue

            # --------------------------------------------------
            # 4. concat + rebuild slice index
            # --------------------------------------------------
            result_table = pa.concat_tables(enriched_tables)

            index = self._build_symbol_slice_index(result_table)

            output_file = output_dir / f"{name}.{self.stage}.parquet"
            pq.write_table(result_table, output_file)

            # --------------------------------------------------
            # 5. commit meta（带 slice capability）
            # --------------------------------------------------
            meta.commit(
                MetaOutput(
                    input_file=input_file,
                    output_file=output_file,
                    rows=result_table.num_rows,
                    index=index,
                )
            )

            logs.info(
                f"[TradeEnrichStep] written {output_file.name} "
                f"symbols={symbol_count} rows={result_table.num_rows}"
            )

        return ctx

    # ------------------------------------------------------------------
    @staticmethod
    def _build_symbol_slice_index(
            table: pa.Table,
    ) -> Dict[str, Tuple[int, int]]:
        """
        构建 symbol -> (start, length)

        前置条件（由本 Step 保证）：
          - table 按 symbol 分块 concat
          - 每个 symbol 内部顺序已保持
        """
        if table.num_rows == 0:
            return {}

        sym = table["symbol"]
        if not pa.types.is_string(sym.type):
            sym = pc.cast(sym, pa.string())

        ree = pc.run_end_encode(sym).combine_chunks()
        values = ree.values.to_pylist()
        run_ends = ree.run_ends.to_pylist()

        index: Dict[str, Tuple[int, int]] = {}
        start = 0
        for symbol, end in zip(values, run_ends):
            end = int(end)
            index[str(symbol)] = (start, end - start)
            start = end

        return index
