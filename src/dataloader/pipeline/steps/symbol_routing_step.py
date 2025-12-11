#!filepath: src/dataloader/pipeline/steps/symbol_routing_step.py

from __future__ import annotations
from pathlib import Path

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.engines.symbol_router_engine import SymbolRouterEngine
from src.adapters.symbol_router_adapter import SymbolRouterAdapter
from src import logs


class SymbolRoutingStep(PipelineStep):

    def __init__(self, symbols, path_manager):
        self.engine = SymbolRouterEngine(symbols)
        self.adapter = SymbolRouterAdapter(path_manager)

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logs.info(f"[SymbolRoutingStep] start date={ctx.date}")

        date_dir = ctx.parquet_dir
        parquet_files = sorted(date_dir.glob("*.parquet"))

        for pq_file in parquet_files:
            kind = self._infer_kind(pq_file.name)
            if not kind:
                continue

            logs.info(f"[SymbolRoutingStep] routing {pq_file.name}")

            # parquet -> batches
            for batch in self.adapter.load_parquet_batches(pq_file):
                splits = self.engine.split_batch_by_symbol(batch)
                if splits:
                    self.adapter.write_symbol_batches(
                        date=ctx.date,
                        kind=kind,
                        symbol_to_batch=splits,
                    )

        logs.info(f"[SymbolRoutingStep] done date={ctx.date}")
        return ctx

    # ----------------------
    @staticmethod
    def _infer_kind(filename: str):
        lower = filename.lower()
        if "order" in lower:
            return "Order"
        if "trade" in lower:
            return "Trade"
        return None
