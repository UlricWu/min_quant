#!filepath: src/dataloader/pipeline/steps/trade_enrich_step.py

from __future__ import annotations

from pathlib import Path

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
from src.utils.path import PathManager
from src import logs


class TradeEnrichStep(PipelineStep):

    def __init__(self, symbols, path_manager: PathManager, burst_ms=5):
        self.engine = TradeEnrichEngine(burst_window_ms=burst_ms)
        self.adapter = TradeEnrichAdapter(path_manager)
        self.symbols = symbols

    def run(self, ctx: PipelineContext) -> PipelineContext:
        date = ctx.date
        logs.info(f"[TradeEnrichStep] start date={date}")

        for symbol in self.symbols:
            sym_dir = ctx.symbol_root / f"{symbol}/{date}"
            trade_path = sym_dir / "Trade.parquet"
            enriched_path = sym_dir / "Trade_Enriched.parquet"

            if not trade_path.exists():
                logs.warning(f"[TradeEnrichStep] symbol={symbol} 无 Trade.parquet，跳过")
                continue

            # 可选跳过
            if enriched_path.exists():
                logs.info(f"[TradeEnrichStep] enriched 已存在 → skip symbol={symbol}")
                continue

            enriched_batches = (
                self.engine.enrich_batch(batch)
                for batch in self.adapter.load_trade_batches(trade_path)
            )

            self.adapter.write_enriched_batches(symbol, date, enriched_batches)

        logs.info(f"[TradeEnrichStep] done date={date}")
        return ctx
