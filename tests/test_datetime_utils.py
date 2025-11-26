#!filepath: tests/test_datetime_utils.py
import pytest
from datetime import datetime
from zoneinfo import ZoneInfo

from src import datetime_utils as dt


def test_parse_int_timestamp_second():
    ts = 1700000000  # 秒
    d = dt.parse(ts)
    assert isinstance(d, datetime)
    assert d.tzinfo == dt.SH_TZ


def test_parse_int_timestamp_millisecond():
    ts = 1700000000000  # 毫秒
    d = dt.parse(ts)
    assert isinstance(d, datetime)
    assert d.tzinfo == dt.SH_TZ


def test_parse_string_datetime():
    s = "2025-01-03 09:30:00.123"
    d = dt.parse(s)
    assert d.hour == 9
    assert d.minute == 30
    assert d.microsecond > 0
    assert d.tzinfo == dt.SH_TZ


def test_timezone_conversion():
    s = "2025-01-03 09:30:00"
    d = dt.parse(s)
    utc = dt.to_utc(d)
    assert utc.tzinfo == ZoneInfo("UTC")


def test_is_trading_time():
    assert dt.is_trading_time("2025-01-03 10:00:00") is True
    assert dt.is_trading_time("2025-01-03 12:00:00") is False


def test_trading_days():
    dt.set_trading_days(["2025-01-03", "2025-01-06"])
    assert dt.is_trading_day("2025-01-03") is True
    assert dt.is_trading_day("2025-01-04") is False


def test_next_prev_trading_day():
    dt.set_trading_days(["2025-01-03", "2025-01-06"])

    nxt = dt.next_trading_day("2025-01-03")
    assert nxt.date().isoformat() == "2025-01-06"

    prev = dt.prev_trading_day("2025-01-06")
    assert prev.date().isoformat() == "2025-01-03"


def test_add_minutes():
    d = dt.parse("2025-01-03 09:30:00")
    new = dt.add_minutes(d, 30)
    assert new.hour == 10
    assert new.minute == 0
