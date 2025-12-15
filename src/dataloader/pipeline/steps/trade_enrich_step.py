from __future__ import annotations

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
from src import logs


class TradeEnrichStep(BasePipelineStep):
    """
    TradeEnrich Step（最终版）

    职责：
    - 决定处理哪些 symbol
    - 调用 Adapter 处理单个 symbol
    """

    def __init__(
        self,
        adapter: TradeEnrichAdapter,
        *,
        symbols: list[str],
        inst=None,
    ):
        super().__init__(inst)
        self.adapter = adapter
        self.symbols = [str(s).zfill(6) for s in symbols]

    def run(self, ctx: PipelineContext) -> PipelineContext:
        date = ctx.date
        symbol_root = ctx.symbol_dir

        for sym in self.symbols:
            symbol_day_dir = symbol_root / sym / date
            if not symbol_day_dir.exists():
                continue

            with self.timed():
                self.adapter.run_for_symbol_day(
                    symbol=sym,
                    date=date,
                    symbol_day_dir=symbol_day_dir,
                )

        return ctx
