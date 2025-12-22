#!filepath: src/pipeline/steps/trade_enrich_step.py
from __future__ import annotations

from pathlib import Path

from src.pipeline.meta import MetaRegistry
from src import logs
import pyarrow as pa
import pyarrow.parquet as pq
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.pipeline.context import EngineContext


class TradeEnrichStep:
    """
    TradeEnrichStep（v1 / trade-only）

    职责：
    - 遍历 symbol
    - 构造 EngineContext
    - 调用 TradeEnrichEngine
    - 负责 IO
    - 负责 MetaRegistry（size-only 校验）
    """

    def __init__(
            self,
            engine: TradeEnrichEngine,
            inst=None,
    ):
        self.engine = engine
        self.inst = inst

    # --------------------------------------------------
    def run(self, ctx) -> None:
        input_dir: Path = ctx.symbol_dir
        input_file = 'trade.parquet'
        out_name = "trade_enriched.parquet"

        # 遍历当日 universe（和 SymbolSplit 完全一致）

        count = 0

        with self.inst.timer(self.__class__.__name__):
            logs.info(f'[TradeEnrichStep] start enriching {input_file}')
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

        logs.info(f'[TradeEnrichStep] process count: {count}')

        return ctx
