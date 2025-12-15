from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator


# =========================
# 输入 / 输出事件定义
# =========================
@dataclass(frozen=True)
class RawTradeEvent:
    ts: datetime
    price: float
    volume: int
    side: str | None   # 'B' / 'S' / None


@dataclass(frozen=True)
class EnrichedTradeEvent:
    ts: datetime
    price: float
    volume: int
    side: str | None
    notional: float
    signed_volume: int


# =========================
# Engine
# =========================
class TradeEnrichEngine:
    """
    Trade 业务增强引擎（最终版）

    职责：
    - 计算 notional
    - 计算 signed_volume
    - 不处理交易所差异
    - 不解析时间
    """

    def process_stream(
        self,
        events: Iterable[RawTradeEvent],
    ) -> Iterator[EnrichedTradeEvent]:

        for ev in events:
            notional = ev.price * ev.volume

            if ev.side == "B":
                signed_volume = ev.volume
            elif ev.side == "S":
                signed_volume = -ev.volume
            else:
                signed_volume = 0

            yield EnrichedTradeEvent(
                ts=ev.ts,
                price=ev.price,
                volume=ev.volume,
                side=ev.side,
                notional=notional,
                signed_volume=signed_volume,
            )
