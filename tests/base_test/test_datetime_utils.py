#!filepath: tests/utils_test/test_datetime_utils.py
from datetime import datetime, date
from zoneinfo import ZoneInfo

from src.utils.datetime_utils import DateTimeUtils as dt

SH_TZ = ZoneInfo("Asia/Shanghai")


# ================================================================
# extract_date()
# ================================================================
def test_extract_date_from_full_datetime():
    assert dt.extract_date("2025-11-07 09:15:00.040") == date(2025, 11, 7)


def test_extract_date_from_slash_format():
    assert dt.extract_date("2025/11/07 09:15:00") == date(2025, 11, 7)


def test_extract_date_from_basic_date():
    assert dt.extract_date("20251107") == date(2025, 11, 7)


def test_extract_date_from_ns_timestamp():
    ns = 1762177123456789000  # 19-digit timestamp
    d = dt.extract_date(ns)
    assert isinstance(d, date)


def test_extract_date_from_datetime_obj():
    dt_input = datetime(2025, 11, 7, 9, 15)
    assert dt.extract_date(dt_input) == date(2025, 11, 7)


# ================================================================
# parse_tick_time()
# ================================================================
# def test_parse_tick_time_7_digits():
#     hh, mm, ss, micro = dt.parse_tick_time("9150004")  # 09:15:00.004
#     assert (hh, mm, ss, micro) == (9, 15, 0, 4000)


def test_parse_tick_time_8_digits():
    hh, mm, ss, micro = dt.parse_tick_time("91500040")  # 09:15:00.040
    assert (hh, mm, ss, micro) == (9, 15, 0, 40000)


def test_parse_tick_time_9_digits():
    hh, mm, ss, micro = dt.parse_tick_time("131500040")  # 13:15:00.040
    assert (hh, mm, ss, micro) == (13, 15, 0, 40000)


def test_parse_tick_time_error_non_digit():
    try:
        dt.parse_tick_time("ABC")
    except ValueError:
        return
    assert False, "非数字 TickTime 应该抛异常"


def test_parse_tick_time_error_length():
    try:
        dt.parse_tick_time("123456")  # < 7 digits
    except ValueError:
        return
    assert False, "TickTime 长度不足应该抛异常"


# ================================================================
# combine_date_tick()
# ================================================================
def test_combine_date_tick():
    d = date(2025, 11, 7)
    tick = (9, 15, 0, 40000)  # 09:15:00.040
    ts = dt.combine_date_tick(d, tick)

    assert ts.year == 2025
    assert ts.month == 11
    assert ts.day == 7
    assert ts.hour == 9
    assert ts.minute == 15
    assert ts.second == 0
    assert ts.microsecond == 40000
    assert ts.tzinfo == SH_TZ


# ================================================================
# parse() TradeTime formats
# ================================================================
def test_parse_trade_time_full_format():
    s = "2025-11-07 09:15:00.040"
    ts = dt.parse(s)
    assert ts.year == 2025
    assert ts.month == 11
    assert ts.day == 7
    assert ts.hour == 9
    assert ts.minute == 15
    assert ts.second == 0
    assert ts.microsecond == 40000
    assert ts.tzinfo == SH_TZ


def test_parse_trade_time_no_millis():
    s = "2025-11-07 09:15:00"
    ts = dt.parse(s)
    assert ts.hour == 9 and ts.minute == 15


def test_parse_trade_time_compact():
    s = "20251107091500"
    ts = dt.parse(s)
    assert ts.year == 2025 and ts.hour == 9


def test_parse_invalid_string():
    try:
        dt.parse("NOT_A_TIME")
    except ValueError:
        return
    assert False, "无效字符串应抛异常"


# ================================================================
# END-TO-END: TradeTime + TickTime → final ts
# ================================================================
def test_end_to_end_tradetime_ticktime():
    trade_time = "2025-11-07 09:00:00.000"
    tick_time = "091500040"  # 09:15:00.040

    d = dt.extract_date(trade_time)             # 2025-11-07
    tick = dt.parse_tick_time(tick_time)        # (9, 15, 0, 40000)
    ts = dt.combine_date_tick(d, tick)

    assert ts.year == 2025
    assert ts.month == 11
    assert ts.day == 7
    assert ts.hour == 9
    assert ts.minute == 15
    assert ts.microsecond == 40000
    assert ts.tzinfo == SH_TZ
