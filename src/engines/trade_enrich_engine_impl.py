#!filepath: src/engines/trade_enrich_engine_impl.py
from __future__ import annotations
from dataclasses import dataclass

import pandas as pd

from src.core.types import TradeBatch
from src.l2.common.event_parser import parse_events
from src.l2.orderbook.trade_enricher import TradeEnricher


@dataclass
class TradeEnrichEngineImpl:
    """
    Atomic Engine：逐笔成交增强引擎（不做 I/O）

    责任：
    - 供应商格式 df → InternalEvent df（parse_events）
    - InternalEvent df → 加 burst / impact（TradeEnricher）
    - 返回新的 TradeBatch（symbol/date 不变）
    """

    burst_window_ms: int = 5

    def __post_init__(self) -> None:
        self._enricher = TradeEnricher(burst_window_ms=self.burst_window_ms)

    def enrich(self, batch: TradeBatch) -> TradeBatch:
        # 1) 把供应商 Trade 表解析成统一事件 schema
        events_df: pd.DataFrame = parse_events(batch.df, kind="trade")

        # 2) 调用你原本的 TradeEnricher 做 burst / impact 等
        enriched_df: pd.DataFrame = self._enricher.enrich(events_df)

        # 3) 返回新的 TradeBatch（仍然是 symbol + date + df）
        return TradeBatch(
            symbol=batch.symbol,
            date=batch.date,
            df=enriched_df,
        )
