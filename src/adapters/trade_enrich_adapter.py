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


class TradeEnrichAdapter(BaseAdapter):
    """
    TradeEnrich Adapter（最终收敛版）

    语义：
    - Adapter 只处理「一个 symbol + 一个 date」
    - 不做 symbol 选择
    - 不持有 symbols
    """

    def __init__(self, engine: TradeEnrichEngine, inst=None) -> None:
        super().__init__(inst)
        self.engine = engine

    # --------------------------------------------------
    # DataFrame → RawTradeEvent
    # --------------------------------------------------

    def _df_to_events(self,
                      df: pd.DataFrame,
                      *,
                      date: str,
                      ) -> Iterable[RawTradeEvent]:

        trade_date = dt.extract_date(date)

        for row in df.itertuples(index=False):
            ts = dt.combine_date_tick(
                trade_date,
                dt.parse_tick_time(row.TickTime),
            )
            price, volume, side = self._extract_trade_fields(row)

            yield RawTradeEvent(
                ts=ts,
                price=price,
                volume=volume,
                side=side,
            )

    # --------------------------------------------------
    def run_for_symbol_day(
            self,
            *,
            symbol: str,
            date: str,
            symbol_day_dir: Path,
    ) -> None:
        """
        处理单个 symbol / 单个 date
        """

        trade_path = symbol_day_dir / "Trade.parquet"
        out_path = symbol_day_dir / "Trade_Enriched.parquet"

        if not trade_path.exists():
            logs.warning(f"[TradeEnrich] {trade_path} 不存在，skip symbol={symbol}")
            return

        if out_path.exists():
            logs.debug(f"[TradeEnrich] 已存在 → skip {out_path}")
            return

        # logs.info(f"[TradeEnrich] enrich symbol={symbol}, date={date}")

        df = pd.read_parquet(trade_path)
        events = self._df_to_events(df, date=date)

        enriched_rows: List[dict] = []

        with self.timer():
            for ev in self.engine.process_stream(events):
                enriched_rows.append(
                    {
                        "ts": ev.ts,
                        "price": ev.price,
                        "volume": ev.volume,
                        "side": ev.side,
                        "notional": ev.notional,
                        "signed_volume": ev.signed_volume,
                    }
                )

        if not enriched_rows:
            logs.warning(f"[TradeEnrich] no output for {symbol} {date}")
            return

        FileSystem.ensure_dir(symbol_day_dir)
        pd.DataFrame(enriched_rows).to_parquet(out_path, index=False)

        logs.debug(
            f"[TradeEnrich] wrote {out_path}, rows={len(enriched_rows)}"
        )

    def _extract_trade_fields(self, row):
        """
        从 SH / SZ Trade row 中提取统一的 price / volume / side
        """
        if hasattr(row, "Price"):  # SH
            price = float(row.Price)
            volume = int(row.Volume)
            side = row.Side if hasattr(row, "Side") else None

        elif hasattr(row, "TradePrice"):  # SZ
            price = float(row.TradePrice)
            volume = int(row.TradeVolume)
            side = None  # SZ trade side 不可靠

        else:
            raise KeyError("Unknown Trade schema")

        return price, volume, side
