#!filepath: src/adapters/normalize_adapter.py
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from src.adapters.base_adapter import BaseAdapter
from src.engines.normalize_engine import NormalizeEngine
from src.engines.context import EngineContext


class NormalizeAdapter(BaseAdapter):
    """
    NormalizeAdapter（正确契约版）

    输入：
        symbol_dir/<symbol>/<date>/{Order,Trade}.parquet
    输出：
        symbol_dir/<symbol>/<date>/{order,trade}/Normalized.parquet
    """

    def __init__(
        self,
        *,
        engine: NormalizeEngine,
        symbols: Iterable[str],
        inst=None,
    ) -> None:
        super().__init__(inst)
        self.engine = engine
        self.symbols = {str(s).zfill(6) for s in symbols}

    def run(self, *, date: str, symbol_dir: Path) -> None:
        for symbol in sorted(self.symbols):
            base = symbol_dir / symbol / date
            if not base.exists():
                continue

            ctx = EngineContext(
                mode="offline",
                symbol=symbol,
                date=date,
                input_path=base,
                output_path=base,  # 输出到 order/ trade 子目录
            )

            with self.timer(f"Normalize_{symbol}"):
                self.engine.execute(ctx)
