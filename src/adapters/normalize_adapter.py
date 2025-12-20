#!filepath: src/adapters/normalize_adapter.py
from __future__ import annotations

from pathlib import Path
from typing import Iterable

from src import FileSystem
from src.adapters.base_adapter import BaseAdapter
from src.engines.normalize_engine import NormalizeEngine
from src.engines.context import EngineContext


class NormalizeAdapter(BaseAdapter):
    """
    NormalizeAdapter（冻结契约版）

    输入：
        /data/parquet/<date>/
            SH_Trade.parquet
            SH_Order.parquet
            SZ_Trade.parquet
            SZ_Order.parquet

    输出：
        /data/canonical/<date>/
            SH_Trade.parquet
            SH_Order.parquet
            SZ_Trade.parquet
            SZ_Order.parquet
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

    def run(self, *, date: str, output_dir: Path, input_dir: Path) -> None:
        with self.timer():
            ctx = EngineContext(
                mode="offline",
                date=date,
                input_path=input_dir,
                output_path=output_dir,
            )

            self.engine.execute(ctx)
