#!filepath: src/adapters/symbol_split_adapter.py
from __future__ import annotations

from src.dataloader.symbol_router import SymbolRouter
from src import logs


class SymbolSplitAdapter:
    """
    负责：
        parquet/<date>/*.parquet → data/symbol/<symbol>/<date>/{Order,Trade}.parquet

    直接调用你现在的 SymbolRouter.route_date(date)
    """

    def __init__(self, router: SymbolRouter):
        self.router = router

    def split_date(self, date: str) -> None:
        logs.info(f"[SymbolSplitAdapter] 开始按 symbol 拆分 date={date}")
        self.router.route_date(date)
        logs.info(f"[SymbolSplitAdapter] 完成拆分 date={date}")
