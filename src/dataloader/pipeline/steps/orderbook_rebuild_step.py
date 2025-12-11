# src/dataloader/pipeline/steps/orderbook_step.py
from __future__ import annotations

from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src.engines.orderbook_engine import OrderBookEngine
from src.adapters.orderbook_adapter import OrderBookAdapter
from src import logs


class OrderBookStep(PipelineStep):
    """
    对所有配置的 symbols，在指定日期重建 OrderBook Snapshot。
    """

    def __init__(self, symbols, path_manager, levels: int = 10):
        self.symbols = [int(s) for s in symbols]
        self.adapter = OrderBookAdapter(path_manager, levels=levels)
        self.engine = OrderBookEngine()

    def run(self, ctx: PipelineContext) -> PipelineContext:
        date = ctx.date
        logs.info(f"[OrderBookStep] start date={date}")

        for sym in self.symbols:
            # ✅ 这里是“调用方法”，不是 for adapter in ...
            self.adapter.build_symbol_orderbook(sym, date, self.engine)

        logs.info(f"[OrderBookStep] done date={date}")
        return ctx
