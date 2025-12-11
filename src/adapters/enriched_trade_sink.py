#!filepath: src/adapters/enriched_trade_sink.py
from __future__ import annotations

from src.core.types import TradeBatch
from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs


class EnrichedTradeSink:
    """
    将 enriched TradeBatch 写回 data/symbol/<symbol>/<date>/Trade_Enriched.parquet
    """

    def __init__(self, path_manager: PathManager):
        self.pm = path_manager

    def save(self, batch: TradeBatch) -> None:
        out_dir = self.pm.symbol_dir(batch.symbol, batch.date)
        FileSystem.ensure_dir(out_dir)

        out_path = out_dir / "Trade_Enriched.parquet"
        batch.df.to_parquet(out_path, index=False)
        logs.debug(f"[EnrichedTradeSink] 写出 {out_path}, rows={len(batch.df)}")
