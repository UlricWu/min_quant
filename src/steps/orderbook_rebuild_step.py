#!filepath: src/steps/orderbook_rebuild_step.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.utils.logger import logs
from src.pipeline.context import EngineContext

class OrderBookRebuildStep(PipelineStep):
    """
    OrderBookRebuildStep（symbol-local, existence-only, Arrow-only, v1）

    Rule:
      if output exists -> SKIP
      else -> RUN
    """

    def __init__(self, engine: OrderBookRebuildEngine, inst=None) -> None:
        self.engine = engine
        self.inst = inst

    # --------------------------------------------------
    def run(self, ctx) -> None:
        input_dir: Path = ctx.symbol_dir
        input_file = 'order.parquet'
        out_name = "orderbook.parquet"

        # 遍历当日 universe（和 SymbolSplit 完全一致）

        count = 0

        with self.inst.timer(self.__class__.__name__):
            logs.info(f'[OrderBookRebuildStep] start rebuilding {input_file}')

            for sym_dir in sorted(input_dir.iterdir()):
                if not sym_dir.is_dir():
                    continue

                file = sym_dir / input_file
                if not file.exists():
                    # logs.warning(f'[TradeEnrichStep] file {file} not found')
                    continue
                count += 1

                output_file = sym_dir / out_name

                if output_file.exists():
                    continue

                ctx_engine = EngineContext(
                    input_path=file,
                    output_path=output_file,
                )

                self.engine.execute(ctx_engine)

        logs.info(f'[OrderBookRebuildStep] process count: {count}')

        return ctx