from datetime import datetime,date, timezone

from src.session.a_share_session import ASharesSessionResolver
import pytest

def test_session_resolver_basic_trading_minute():
    """
    Contract:
    交易时间 minute → 正确 trading_date
    """
    resolver = ASharesSessionResolver()

    # 2025-01-02 09:30 CST = 2025-01-02 01:30 UTC
    minute = datetime(2025, 1, 2, 1, 30)

    key = resolver.trading_day_of(minute)

    assert key.exchange == "SSE"
    assert key.trading_date == date(2025, 1, 2)


def test_session_resolver_lunch_break_rejected():
    """
    Contract:
    午休时间不属于任何 TradingDay
    """
    resolver = ASharesSessionResolver()

    # 12:00 CST = 04:00 UTC
    minute = datetime(2025, 1, 2, 4, 0)

    with pytest.raises(ValueError):
        resolver.trading_day_of(minute)
def test_session_resolver_non_trading_time_rejected():
    """
    Contract:
    非交易时间 → 明确失败，而不是 silent mapping
    """
    resolver = ASharesSessionResolver()

    # 08:00 CST
    minute = datetime(2025, 1, 2, 0, 0)

    with pytest.raises(ValueError):
        resolver.trading_day_of(minute)
def test_session_resolver_requires_naive_datetime():
    """
    🔒 Contract:
    SessionResolver 只接受 naive datetime
    tz 语义必须统一在 resolver 内部
    """
    resolver = ASharesSessionResolver()

    aware = datetime(
        2025, 1, 2, 1, 30,
        tzinfo=timezone.utc,
    )

    with pytest.raises(ValueError):
        resolver.trading_day_of(aware)
def test_session_resolver_cross_day_mapping():
    """
    Contract:
    TradingDay ≠ calendar day (UTC)
    """
    resolver = ASharesSessionResolver()

    # 2025-01-01 23:59 UTC
    # → 2025-01-02 07:59 CST（开盘前，非交易）
    minute = datetime(2025, 1, 1, 23, 59)

    with pytest.raises(ValueError):
        resolver.trading_day_of(minute)

    # 2025-01-02 01:30 UTC = 09:30 CST
    minute2 = datetime(2025, 1, 2, 1, 30)
    key = resolver.trading_day_of(minute2)

    assert key.trading_date == date(2025, 1, 2)
