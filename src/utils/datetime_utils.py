#!filepath: src/utils/datetime_utils.py
from __future__ import annotations
from datetime import datetime, timedelta, time
from typing import Optional, List, Union
from zoneinfo import ZoneInfo


class DateTimeUtils:
    """
    统一的时间解析、时区转换、交易时间判断等工具。
    ZoneInfo 版本，不再依赖 pytz。
    """

    SH_TZ = ZoneInfo("Asia/Shanghai")
    trading_days: List[datetime.date] = []

    # ---------------------------------------------------------
    # 统一解析时间：int / str / datetime → datetime(tz=Asia/Shanghai)
    # ---------------------------------------------------------
    @classmethod
    def parse(cls, ts: Union[int, str, datetime]) -> datetime:
        """
        自动识别时间格式：
        - int: 秒 / 毫秒 / 微秒 / 纳秒
        - str: 若干常见日期格式
        - datetime: 保持时区一致
        """

        # Already datetime
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                return ts.replace(tzinfo=cls.SH_TZ)
            return ts.astimezone(cls.SH_TZ)

        # -----------------------------
        # int → timestamp
        # -----------------------------
        if isinstance(ts, int):
            ts_str = str(ts)
            if len(ts_str) == 10:       # 秒
                return datetime.fromtimestamp(ts, cls.SH_TZ)
            elif len(ts_str) == 13:     # 毫秒
                return datetime.fromtimestamp(ts / 1000, cls.SH_TZ)
            elif len(ts_str) == 16:     # 微秒
                return datetime.fromtimestamp(ts / 1_000_000, cls.SH_TZ)
            elif len(ts_str) == 19:     # 纳秒
                return datetime.fromtimestamp(ts / 1_000_000_000, cls.SH_TZ)
            else:
                raise ValueError(f"无法识别的时间戳长度: {ts}")

        # -----------------------------
        # str → datetime
        # -----------------------------
        if isinstance(ts, str):

            # 完整日期 + 时间
            fmts_full = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y%m%d %H:%M:%S",
                "%Y%m%d %H:%M:%S.%f",
                "%Y%m%d%H%M%S",
            ]

            for fmt in fmts_full:
                try:
                    dt = datetime.strptime(ts, fmt)
                    return dt.replace(tzinfo=cls.SH_TZ)
                except Exception:
                    pass

            # 纯日期格式（无时分秒）
            fmts_date = ["%Y-%m-%d", "%Y%m%d"]
            for fmt in fmts_date:
                try:
                    d = datetime.strptime(ts, fmt).date()
                    return datetime.combine(d, time.min).replace(tzinfo=cls.SH_TZ)
                except Exception:
                    pass

            raise ValueError(f"无法解析时间字符串: {ts}")

        raise TypeError(f"不支持的时间类型: {type(ts)}")

    # ---------------------------------------------------------
    # 时区转换
    # ---------------------------------------------------------
    @classmethod
    def to_shanghai(cls, dt: datetime) -> datetime:
        return cls.parse(dt)

    @classmethod
    def to_utc(cls, dt: datetime) -> datetime:
        dt = cls.parse(dt)
        return dt.astimezone(ZoneInfo("UTC"))

    # ---------------------------------------------------------
    # A股交易时间判断
    # ---------------------------------------------------------
    @classmethod
    def is_trading_time(cls, dt: datetime) -> bool:
        dt = cls.parse(dt)
        t = dt.time()

        # todo 集合竞价
        # 上午 09:30 - 11:30
        if time(9, 30) <= t <= time(11, 30):
            return True
        # 下午 13:00 - 15:00
        if time(13, 0) <= t <= time(15, 0):
            return True

        return False

    # ---------------------------------------------------------
    # 交易日操作
    # ---------------------------------------------------------
    @classmethod
    def set_trading_days(cls, days: List[Union[str, datetime]]):
        parsed = [cls.parse(d).date() for d in days]
        cls.trading_days = sorted(parsed)

    @classmethod
    def is_trading_day(cls, dt: Union[str, datetime]) -> bool:
        if not cls.trading_days:
            raise ValueError("未设置 trading_days")
        return cls.parse(dt).date() in cls.trading_days

    @classmethod
    def next_trading_day(cls, dt: Union[str, datetime]) -> datetime:
        if not cls.trading_days:
            raise ValueError("未设置 trading_days")

        d = cls.parse(dt).date()
        for td in cls.trading_days:
            if td > d:
                return datetime.combine(td, time.min).replace(tzinfo=cls.SH_TZ)
        raise ValueError("没有下一个交易日")

    @classmethod
    def prev_trading_day(cls, dt: Union[str, datetime]) -> datetime:
        if not cls.trading_days:
            raise ValueError("未设置 trading_days")

        d = cls.parse(dt).date()
        prev = None
        for td in cls.trading_days:
            if td < d:
                prev = td

        if prev:
            return datetime.combine(prev, time.min).replace(tzinfo=cls.SH_TZ)
        raise ValueError("没有前一个交易日")

    # ---------------------------------------------------------
    # 时间偏移
    # ---------------------------------------------------------
    @classmethod
    def add_minutes(cls, dt: Union[str, datetime], minutes: int) -> datetime:
        dt = cls.parse(dt)
        return dt + timedelta(minutes=minutes)
