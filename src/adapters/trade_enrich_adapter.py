#!filepath: src/adapters/trade_enrich_adapter.py
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List

import pandas as pd

from src.engines.trade_enrich_engine import TradeEnrichEngine, RawTradeEvent, EnrichedTradeEvent
from src.adapters.base import BaseAdapter
from src.utils.path import PathManager
from src.utils.filesystem import FileSystem
from src import logs


class TradeEnrichAdapter(BaseAdapter):
    """
    示例 Adapter（Offline 模式）：

    - 输入：data/symbol/<symbol>/<date>/Trade.parquet
    - 输出：data/symbol/<symbol>/<date>/Trade_Enriched.parquet

    注意：
    - 这里只是模板版本，真实逻辑可以后续替换为 Arrow/streaming
    """

    def __init__(self, engine: TradeEnrichEngine, pm: PathManager, symbols: list[int]) -> None:
        super().__init__(engine)
        self.pm = pm
        self.symbols = [int(s) for s in symbols]

    # 工具：将 DataFrame 行转为 RawTradeEvent
    @staticmethod
    def _df_to_events(df: pd.DataFrame) -> Iterable[RawTradeEvent]:
        for row in df.itertuples(index=False):
            yield RawTradeEvent(
                ts_ns=int(getattr(row, "ts_ns")),
                price=float(getattr(row, "price")),
                volume=int(getattr(row, "volume")),
                side=getattr(row, "side", None),
            )

    # 工具：把 EnrichedTradeEvent 列表转为 DataFrame
    @staticmethod
    def _events_to_df(events: Iterable[EnrichedTradeEvent]) -> pd.DataFrame:
        rows: List[dict] = []
        for ev in events:
            rows.append(
                {
                    "ts_ns": ev.ts_ns,
                    "price": ev.price,
                    "volume": ev.volume,
                    "side": ev.side,
                    "notional": ev.notional,
                    "signed_volume": ev.signed_volume,
                }
            )
        return pd.DataFrame(rows)

    # Offline 入口：处理某个日期所有 symbol
    def run_for_date(self, date: str) -> None:
        logs.info(f"[TradeEnrichAdapter] run_for_date date={date}")

        for sym in self.symbols:
            sym_dir = self.pm.symbol_dir(sym, date)
            trade_path = sym_dir / "Trade.parquet"
            enriched_path = sym_dir / "Trade_Enriched.parquet"

            if not trade_path.exists():
                logs.warning(f"[TradeEnrichAdapter] {trade_path} 不存在，跳过 symbol={sym}")
                continue

            if enriched_path.exists():
                logs.info(f"[TradeEnrichAdapter] enriched 已存在 → skip symbol={sym}")
                continue

            logs.info(f"[TradeEnrichAdapter] enrich symbol={sym}, date={date}")

            df = pd.read_parquet(trade_path)
            events = self._df_to_events(df)
            enriched_iter = self.engine.process_stream(events)
            df_out = self._events_to_df(enriched_iter)

            FileSystem.ensure_dir(sym_dir)
            df_out.to_parquet(enriched_path, index=False)

            logs.info(
                f"[TradeEnrichAdapter] 写出 {enriched_path}, rows={len(df_out)}"
            )
