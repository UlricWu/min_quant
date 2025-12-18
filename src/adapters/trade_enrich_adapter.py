from __future__ import annotations

from pathlib import Path
from typing import Iterable, List
import pandas as pd

from src.engines.trade_enrich_engine import (
    TradeEnrichEngine,
    RawTradeEvent,
    EnrichedTradeEvent,
)
from src.adapters.base_adapter import BaseAdapter
from src.utils.datetime_utils import DateTimeUtils as dt
from src.utils.filesystem import FileSystem
from src import logs

from src.engines.context import EngineContext
class TradeEnrichAdapter(BaseAdapter):
    """
    TradeEnrich Adapter（最终收敛版）

    语义：
    - 遍历 symbol
    - 构造 EngineContext
    - 调用 engine.execute(ctx)
    """

    def __init__(
        self,
        engine: TradeEnrichEngine,
        *,
        symbols: list[str],
        inst=None,
    ):
        super().__init__(inst)
        self.engine = engine
        self.symbols = [str(s).zfill(6) for s in symbols]

    # --------------------------------------------------
    def run(
        self,
        *,
        date: str,
        symbol_root: Path,
    ) -> None:
        for sym in self.symbols:
            sym_dir = symbol_root / sym / date
            input_path = sym_dir / "Events.parquet"
            output_path = sym_dir / "Trade_Enriched.parquet"

            if not input_path.exists():
                logs.warning(f"[TradeEnrich] {input_path} 不存在，skip symbol={sym}")
                continue

            if output_path.exists():
                logs.info(f"[TradeEnrich] enriched 已存在 → skip symbol={sym}")
                continue

            ctx = EngineContext(
                mode="offline",
                symbol=sym,
                date=date,
                input_path=input_path,
                output_path=output_path,
            )

            # Adapter 级 timer（不进 timeline）
            with self.timer("trade_enrich_symbol"):
                self.engine.execute(ctx)
