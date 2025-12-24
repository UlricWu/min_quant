# src/session/a_share_session.py
from datetime import datetime, date, time, timedelta, timezone

from src.session.session_resolver import TradingDayKey


class ASharesSessionResolver:
    """
    A 股 SessionResolver（简化版）

    规则（冻结测试用）：
      - 时区：Asia/Shanghai (UTC+8)
      - 交易时段：
          09:30–11:30
          13:00–15:00
      - 非交易时间 → ValueError
    """

    TZ = timezone(timedelta(hours=8))

    def trading_day_of(self, minute: datetime) -> TradingDayKey:
        if minute.tzinfo is not None:
            raise ValueError("minute must be naive datetime (UTC semantics)")

        # attach tz
        local = minute.replace(tzinfo=timezone.utc).astimezone(self.TZ)
        t = local.time()

        if not self._is_trading_time(t):
            raise ValueError(f"Non-trading minute: {local}")

        trading_date = local.date()
        return TradingDayKey(exchange="SSE", trading_date=trading_date)

    @staticmethod
    def _is_trading_time(t: time) -> bool:
        return (
            time(9, 30) <= t < time(11, 30)
            or time(13, 0) <= t < time(15, 0)
        )
