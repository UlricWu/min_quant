#!filepath: src/adapters/parquet_trade_event_source.py
from __future__ import annotations
from pathlib import Path
from typing import Iterable, Iterator

import pandas as pd

from src.core.types import TradeBatch
from src.utils.path import PathManager
from src import logs


class ParquetTradeEventSource:
    """
    从 data/symbol/<symbol>/<date>/Trade.parquet 读取出 TradeBatch，
    提供给 Atomic Engine（TradeEnrichEngineImpl）使用。
    """

    def __init__(self, path_manager: PathManager, symbols: Iterable[int | str]):
        self.pm = path_manager
        self.symbols = [int(s) for s in symbols]

    def iter_symbol_date(self, date: str) -> Iterator[TradeBatch]:
        for sym in self.symbols:
            sym_dir = self.pm.symbol_dir(sym, date)
            trade_path = sym_dir / "Trade.parquet"

            if not trade_path.exists():
                logs.debug(f"[ParquetTradeEventSource] {trade_path} 不存在 → 跳过")
                continue

            df = pd.read_parquet(trade_path)
            yield TradeBatch(
                symbol=f"{sym:06d}",
                date=date,
                df=df,
            )
