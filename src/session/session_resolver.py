# src/session/session_resolver.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from typing import Protocol


@dataclass(frozen=True)
class TradingDayKey:
    exchange: str           # e.g. "SSE", "SZSE"
    trading_date: date      # 交易日（业务语义）


class SessionResolver(Protocol):
    """
    SessionResolver Contract (Frozen)

    唯一职责：
      - 将 minute（物理时间）映射为 TradingDayKey
    """

    def trading_day_of(self, minute: datetime) -> TradingDayKey:
        """
        Parameters
        ----------
        minute : datetime
            naive datetime，语义等价于 UTC

        Returns
        -------
        TradingDayKey

        Raises
        ------
        ValueError
            若 minute 不属于任何交易时段
        """
        ...
