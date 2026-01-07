#!filepath: src/pipeline/steps/minute_trade_agg_step.py
from __future__ import annotations

from pathlib import Path
from typing import List

import pyarrow as pa

from src.pipeline.step import PipelineStep
from src.data_system.context import DataContext
from src.data_system.engines.minute_trade_agg_engine import MinuteTradeAggEngine
from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource
from src.utils.logger import logs

from src.data_system.engines.symbol_index_engine import SymbolIndexEngine
from src.utils.parquet_writer import ParquetAppendWriter


class MinuteTradeAggStep(PipelineStep):
    """
    MinuteTradeAggStep（FINAL / FROZEN）

    输入：
      fact/enriched.*trade.parquet   （multi-symbol, event-level）

    输出：
      fact/min.*trade.parquet        （multi-symbol, minute-level）

    冻结前置条件（由本 Step 保证）：
      - enriched 表已按 symbol 分块 concat
      - 每个 symbol 内部 ts 已升序

    冻结原则：
      - orchestration only
      - engine 只处理单 symbol
      - min 阶段重新生成 slice index
      - 统一 writer / index engine
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
    def run(self, ctx: DataContext) -> DataContext:
        input_dir: Path = ctx.fact_dir
        meta_dir: Path = ctx.meta_dir
        output_dir: Path = ctx.fact_dir

        # 只处理 trade
        for input_file in input_dir.glob(f"{self.upstream_stage}.*trade.parquet"):
            name = input_file.stem.split(".")[1]
            output_file = output_dir / f"{self.stage}.{name}.parquet"

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
            # 2. SliceSource（来自 enriched 的 slice capability）
            # --------------------------------------------------
            source = SliceSource(
                meta_dir=meta_dir,
                stage=self.upstream_stage,
                output_slot=name,
            )

            minute_tables: List[pa.Table] = []

            # --------------------------------------------------
            # 3. per-symbol minute aggregation（engine 纯计算）
            # --------------------------------------------------
            with self.inst.timer(f"[{self.stage}] {name}"):
                symbol_count = 0

                for symbol, sub in source:
                    if sub.num_rows == 0:
                        continue

                    minute = self.engine.execute(sub)
                    if minute.num_rows == 0:
                        continue

                    # engine 不负责 symbol，Step 补齐
                    minute = minute.append_column(
                        "symbol",
                        pa.array([symbol] * minute.num_rows),
                    )

                    minute_tables.append(minute)
                    symbol_count += 1

            if not minute_tables:
                logs.warning(f"[{self.stage}] {name} no minute data")
                continue

            # --------------------------------------------------
            # 4. concat + canonical sort + rebuild slice index
            # --------------------------------------------------
            tables, index = SymbolIndexEngine.execute(
                pa.concat_tables(minute_tables)
            )

            writer = ParquetAppendWriter(output_file=output_file)
            writer.write(tables)
            writer.close()

            # --------------------------------------------------
            # 5. commit meta（带 slice capability）
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
                f"symbols={symbol_count} rows={tables.num_rows}"
            )

        return ctx
