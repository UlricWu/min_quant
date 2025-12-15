from __future__ import annotations

from src.dataloader.pipeline.step import BasePipelineStep
from src.dataloader.pipeline.context import PipelineContext
from src.adapters.orderbook_adapter import OrderBookRebuildAdapter
from src import logs


class OrderBookRebuildStep(BasePipelineStep):
    """
    Step：
    - 不做业务
    - 不关心 SH/SZ
    - 只是控制「是否 / 对哪些 symbol 跑」
    """

    def __init__(
        self,
        adapter: OrderBookRebuildAdapter,
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
            sym_dir = symbol_root / sym / date
            order_p = sym_dir / "Order.parquet"
            trade_p = sym_dir / "Trade.parquet"
            out_p = sym_dir

            if not sym_dir.exists():
                continue

            with self.timed():  # Step 级语义边界（可 record=False）
                self.adapter.rebuild_symbol_day(
                    symbol=sym,
                    date=date,
                    order_path=order_p,
                    trade_path=trade_p,
                    out_path=out_p,
                )

        return ctx
