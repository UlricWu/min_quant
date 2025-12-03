#!filepath: src/utils/datetime_utils.py
from __future__ import annotations
from datetime import datetime, timedelta, time, date
from typing import List, Union
from zoneinfo import ZoneInfo


class DateTimeUtils:
    SH_TZ = ZoneInfo("Asia/Shanghai")
    trading_days: List[date] = []

    # ================================================================
    # 🔥 从 TradeTime 中提取日期（供应商提供完整字符串）
    # ================================================================
    @classmethod
    def extract_date(cls, trade_time: Union[str, datetime, int]) -> date:
        """
        TradeTime 输入可能为：
            "2025-11-07 09:15:00.040"
            "2025/11/07 09:15:00"
            "20251107"
            1762177123456789000    # ns timestamp
        """
        # int → datetime
        if isinstance(trade_time, int):
            return cls.parse(trade_time).date()

        # datetime 直接取日期
        if isinstance(trade_time, datetime):
            return trade_time.date()

        s = str(trade_time).strip()

        # YYYY-MM-DD / YYYY/MM/DD
        for fmt in ["%Y-%m-%d", "%Y/%m/%d"]:
            try:
                return datetime.strptime(s[:10], fmt).date()
            except Exception:
                pass

        # YYYYMMDD
        if len(s) >= 8 and s[:8].isdigit():
            try:
                return datetime.strptime(s[:8], "%Y%m%d").date()
            except Exception:
                pass

        # 完整 datetime 字符串
        for fmt in [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S",
        ]:
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                pass

        raise ValueError(f"无法从 TradeTime 提取日期: {trade_time}")

    # ================================================================
    # 🔥 TickTime（如 91500060）→ (hh, mm, ss, microsec)
    # ================================================================
    @classmethod
    def parse_tick_time(cls, t: Union[int, str]) -> tuple:
        """
        tick 格式：HHMMSSmmm
        长度：7~9 位
        """
        s = str(t).strip()
        if not s.isdigit() or not (8 <= len(s) <= 9):
            raise ValueError(f"TickTime 格式错误: {t}")

        # 从后往前拆
        mmm = int(s[-3:])
        ss  = int(s[-5:-3])
        mm  = int(s[-7:-5])
        hh  = int(s[:-7]) if s[:-7] else int(s[-7])

        return hh, mm, ss, mmm * 1000   # 返回微秒值

    # ================================================================
    # 🔥 合成最终 ts：日期 + TickTime
    # ================================================================
    @classmethod
    def combine_date_tick(cls, d: date, tick: tuple) -> datetime:
        hh, mm, ss, micros = tick
        return datetime(d.year, d.month, d.day, hh, mm, ss, micros, tzinfo=cls.SH_TZ)

    # ================================================================
    # parse() 用于解析 TradeTime（完整时间字符串）或 timestamp
    # ================================================================
    @classmethod
    def parse(cls, ts: Union[int, str, datetime]) -> datetime:
        if isinstance(ts, datetime):
            return ts.astimezone(cls.SH_TZ) if ts.tzinfo else ts.replace(tzinfo=cls.SH_TZ)

        # int timestamp
        if isinstance(ts, int):
            s = str(ts)
            if len(s) == 10:   # 秒
                return datetime.fromtimestamp(ts, cls.SH_TZ)
            if len(s) == 13:
                return datetime.fromtimestamp(ts / 1000, cls.SH_TZ)
            if len(s) == 16:
                return datetime.fromtimestamp(ts / 1_000_000, cls.SH_TZ)
            if len(s) == 19:
                return datetime.fromtimestamp(ts / 1_000_000_000, cls.SH_TZ)
            raise ValueError(f"无法识别的整数时间戳: {ts}")

        # 字符串 → datetime
        if isinstance(ts, str):
            ts = ts.strip()
            fmts = [
                "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y/%m/%d %H:%M:%S.%f",
                "%Y/%m/%d %H:%M:%S",
                "%Y%m%d%H%M%S",
            ]
            for fmt in fmts:
                try:
                    dtime = datetime.strptime(ts, fmt)
                    return dtime.replace(tzinfo=cls.SH_TZ)
                except Exception:
                    pass

            raise ValueError(f"无法解析时间字符串: {ts}")

        raise TypeError(f"不支持的时间类型: {type(ts)}")

    # ---------------------------------------------------------------
    # trading time/day operations（保持原样）
    # ---------------------------------------------------------------
    @classmethod
    def is_trading_time(cls, dt_: datetime) -> bool:
        dt_ = cls.parse(dt_)
        t = dt_.time()
        return (time(9, 30) <= t <= time(11, 30)) or (time(13, 0) <= t <= time(15, 0))

    @classmethod
    def set_trading_days(cls, days: List[Union[str, datetime]]):
        cls.trading_days = sorted([cls.parse(d).date() for d in days])

    @classmethod
    def is_trading_day(cls, dt_: Union[str, datetime]) -> bool:
        if not cls.trading_days:
            raise ValueError("未设置 trading_days")
        return cls.parse(dt_).date() in cls.trading_days

    @classmethod
    def next_trading_day(cls, dt_: Union[str, datetime]) -> datetime:
        if not cls.trading_days:
            raise ValueError("未设置 trading_days")
        d = cls.parse(dt_).date()
        for td in cls.trading_days:
            if td > d:
                return datetime.combine(td, time.min).replace(tzinfo=cls.SH_TZ)
        raise ValueError("没有下一个交易日")

    @classmethod
    def prev_trading_day(cls, dt_: Union[str, datetime]) -> datetime:
        if not cls.trading_days:
            raise ValueError("未设置 trading_days")
        d = cls.parse(dt_).date()
        prev = None
        for td in cls.trading_days:
            if td < d:
                prev = td
        if prev:
            return datetime.combine(prev, time.min).replace(tzinfo=cls.SH_TZ)
        raise ValueError("没有前一个交易日")

    @classmethod
    def add_minutes(cls, dt_: Union[str, datetime], minutes: int) -> datetime:
        dt_ = cls.parse(dt_)
        return dt_ + timedelta(minutes=minutes)
