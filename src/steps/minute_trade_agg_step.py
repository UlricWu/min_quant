# src/pipeline/steps/minute_trade_agg_step.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine
from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource
from src.utils.logger import logs


class MinuteTradeAggStep(PipelineStep):
    """
    MinuteTradeAggStep（FINAL / FROZEN）

    输入：
      fact/*.enriched.parquet   （multi-symbol, event-level）

    输出：
      fact/*.min.parquet        （multi-symbol, minute-level）

    冻结前置条件（由本 Step 保证）：
      - enriched 表已按 symbol 分块 concat
      - 每个 symbol 内部 ts 已升序

    冻结原则：
      - orchestration only
      - engine 只处理单 symbol
      - min 阶段重新生成 slice index
    """

    stage = "min"
    upstream_stage = "enriched"

    def __init__(
        self,
        engine: MinuteTradeAggEngine,
        inst=None,
    ) -> None:
        super().__init__(inst)
        self.engine = engine

    # ------------------------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        input_dir: Path = ctx.fact_dir
        meta_dir: Path = ctx.meta_dir

        for input_file in input_dir.glob(f"*trade.{self.upstream_stage}.parquet"):
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
                logs.warning(
                    f"[{self.stage}] meta hit → skip {input_file.name}"
                )
                continue

            # --------------------------------------------------
            # 2. 构造 SliceSource（来自 enriched 的 slice capability）
            # --------------------------------------------------
            source = SliceSource(
                meta_dir=meta_dir,
                stage=self.upstream_stage,
                output_slot=name,
            )

            minute_tables: list[pa.Table] = []

            # --------------------------------------------------
            # 3. per-symbol minute aggregation
            # --------------------------------------------------
            with self.inst.timer(f"[{self.stage}] {name}"):
                symbol_count = 0

                for symbol, sub in source:
                    if sub.num_rows == 0:
                        continue

                    minute = self.engine.execute(sub)
                    if minute.num_rows == 0:
                        continue

                    minute = minute.append_column(
                        "symbol",
                        pa.array([symbol] * minute.num_rows)
                    )

                    minute_tables.append(minute)
                    symbol_count += 1

            if not minute_tables:
                logs.warning(f"[{self.stage}] {name} no minute data")
                continue

            # --------------------------------------------------
            # 4. concat + rebuild slice index
            # --------------------------------------------------
            result_table = pa.concat_tables(minute_tables)

            index = self._build_symbol_slice_index(result_table)

            output_file = input_dir / f"{name}.{self.stage}.parquet"
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
                f"[{self.stage}] written {output_file.name} "
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
