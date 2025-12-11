#!filepath: src/engines/trade_enrich_engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Iterable

from src.engines.base import BaseEngine


@dataclass
class RawTradeEvent:
    ts_ns: int
    price: float
    volume: int
    side: Optional[str]  # 'B'/'S'/None


@dataclass
class EnrichedTradeEvent:
    ts_ns: int
    price: float
    volume: int
    side: Optional[str]
    notional: float
    signed_volume: int


class TradeEnrichEngine(BaseEngine[RawTradeEvent, EnrichedTradeEvent]):
    """
    示例 Engine：逐笔成交增强（非常简化版）。
    """

    def process(self, event: RawTradeEvent) -> EnrichedTradeEvent:
        sign = 0
        if event.side == "B":
            sign = 1
        elif event.side == "S":
            sign = -1

        signed_volume = sign * event.volume
        notional = event.price * event.volume

        return EnrichedTradeEvent(
            ts_ns=event.ts_ns,
            price=event.price,
            volume=event.volume,
            side=event.side,
            notional=notional,
            signed_volume=signed_volume,
        )

    # 如果你需要跨事件状态（簇、impact 等），可以覆写 process_stream：
    def process_stream(self, events: Iterable[RawTradeEvent]) -> Iterable[EnrichedTradeEvent]:
        last_price: Optional[float] = None
        for ev in events:
            # 在这里可以写 burst_id / impact 等状态逻辑
            # 现在先简单直接调用 process
            enriched = self.process(ev)
            last_price = ev.price
            yield enriched
