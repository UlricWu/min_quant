# #!filepath: src/engines/minute_trade_agg_engine.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pyarrow as pa
import pyarrow.compute as pc

US_PER_MINUTE = 60 * 1_000_000
US_PER_HOUR = 3600 * 1_000_000
US_PER_DAY = 24 * 3600 * 1_000_000

_EXCHANGE_OFFSET_US = {
    "CN": 8 * US_PER_HOUR,  # Asia/Shanghai
    "SH": 8 * US_PER_HOUR,
    "SZ": 8 * US_PER_HOUR,

    # 预留但不启用
    # "US": -5 * US_PER_HOUR,
}


# =============================================================================
# Arrow-safe math helpers
# =============================================================================
def _mod(a: pa.Array, b: int) -> pa.Array:
    """
    Arrow-safe modulo:
        a % b == a - floor(a / b) * b

    Notes
    -----
    - Do NOT use pc.mod / pc.remainder (not stable across versions)
    - b must be int
    """
    return pc.subtract(
        a,
        pc.multiply(
            pc.cast(
                pc.floor(pc.divide(a, pa.scalar(b, pa.int64()))),
                pa.int64(),
            ),
            pa.scalar(b, pa.int64()),
        ),
    )


def _epoch_days_to_yyyymmdd(days: int) -> int:
    """
    Convert days since Unix epoch to YYYYMMDD (UTC-based, deterministic).

    This is intentionally done in Python for correctness and clarity.
    """
    dt = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(days=days)
    return dt.year * 10000 + dt.month * 100 + dt.day


# =============================================================================
# Engine
# =============================================================================
class MinuteTradeAggEngine:
    """
    MinuteTradeAggEngine（研究友好 · 最终冻结版）

    输入：
      - enriched trade Arrow Table（单 symbol，已按 ts 升序）
      - 至少包含列：
          - ts        : int64 (epoch microseconds, absolute instant)
          - price     : float
          - volume    : int
          - notional  : float

    输出：
      - minute trade Arrow Table（OHLCV + 时间维度）

        Columns:
          - minute_local_us : int64
              交易所【本地钟面】minute 起点（epoch microseconds）
          - trade_date      : int32   (YYYYMMDD)
          - minute          : int16   (HHMM, e.g. 932)
          - minute_str      : string  ("09:32")

          - open            : float
          - high            : float
          - low             : float
          - close           : float
          - volume          : int64
          - notional        : float64
          - trade_count     : int64

    冻结语义：
      - ts 是 absolute instant（infra 真相）
      - minute_local_us 是交易所本地钟面时间（唯一时间真相）
      - trade_date / minute / minute_str 仅用于研究与 debug
      - 本 Engine 不输出 timestamp
      - China 市场假设 UTC+8，无 DST
    """

    # ------------------------------------------------------------------
    # Constants (China market, no DST)
    # ------------------------------------------------------------------

    _US_PER_MINUTE = 60 * 1_000_000

    def __init__(self, exchange: str = "CN") -> None:
        try:
            self._offset_us = _EXCHANGE_OFFSET_US[exchange]
        except KeyError:
            raise ValueError(f"Unsupported exchange: {exchange}")

    # ------------------------------------------------------------------
    def execute(self, table: pa.Table) -> pa.Table:
        if table.num_rows == 0:
            return table

        self._assert_sorted_ts(table["ts"])

        ts = table["ts"]  # int64 epoch us

        # --------------------------------------------------
        # 1. absolute time → local wall-clock time axis
        # --------------------------------------------------
        ts_local_us = pc.add(
            ts,
            pa.scalar(self._offset_us, pa.int64()),
        )

        # --------------------------------------------------
        # 2. local minute alignment (core truth)
        # --------------------------------------------------
        minute_local_us = pc.multiply(
            pc.cast(
                pc.floor(pc.divide(ts_local_us, pa.scalar(US_PER_MINUTE))),
                pa.int64(),
            ),
            pa.scalar(US_PER_MINUTE),
        )

        table = table.append_column("minute_local_us", minute_local_us)

        # --------------------------------------------------
        # 3. group by minute_local_us
        # --------------------------------------------------
        grouped = (
            table
            .group_by("minute_local_us", use_threads=False)
            .aggregate(
                [
                    ("price", "first"),  # open
                    ("price", "max"),  # high
                    ("price", "min"),  # low
                    ("price", "last"),  # close
                    ("volume", "sum"),
                    ("notional", "sum"),
                    ("price", "count"),  # trade_count
                ]
            )
        )

        # --------------------------------------------------
        # 4. derive research-friendly time dimensions
        # --------------------------------------------------
        t = grouped["minute_local_us"]

        # seconds since local midnight
        t_day = _mod(t, US_PER_DAY)

        # hour = (t % day) // hour
        hour = pc.cast(
            pc.floor(
                pc.divide(
                    t_day,
                    pa.scalar(US_PER_HOUR, pa.int64()),
                )
            ),
            pa.int64(),
        )

        # minute = (t % hour) // minute
        t_hour = _mod(t, US_PER_HOUR)

        minute = pc.cast(
            pc.floor(
                pc.divide(
                    t_hour,
                    pa.scalar(US_PER_MINUTE, pa.int64()),
                )
            ),
            pa.int64(),
        )

        # HHMM
        minute_hhmm = pc.add(
            pc.multiply(hour, pa.scalar(100, pa.int64())),
            minute,
        )

        # trade_date: derive from local wall-clock epoch (days since epoch)
        days = pc.cast(
            pc.floor(
                pc.divide(
                    t,
                    pa.scalar(US_PER_DAY, pa.int64()),
                )
            ),
            pa.int64(),
        )

        trade_date = pa.array(
            [_epoch_days_to_yyyymmdd(d) for d in days.to_pylist()],
            type=pa.int32(),
        )

        minute_str = pa.array(
            [f"{h:02d}:{m:02d}" for h, m in zip(hour.to_pylist(), minute.to_pylist())],
            type=pa.string(),
        )

        # --------------------------------------------------
        # 5. final output schema (frozen)
        # --------------------------------------------------
        return pa.table(
            {
                "ts": grouped["minute_local_us"],
                "trade_date": trade_date,
                "minute": pc.cast(minute_hhmm, pa.int16()),
                "minute_str": minute_str,

                "open": grouped["price_first"],
                "high": grouped["price_max"],
                "low": grouped["price_min"],
                "close": grouped["price_last"],
                "volume": grouped["volume_sum"],
                "notional": grouped["notional_sum"],
                "trade_count": grouped["price_count"],
            }
        )

    # ------------------------------------------------------------------
    @staticmethod
    def _assert_sorted_ts(ts: pa.Array) -> None:
        """
        Require input sorted by ts (non-decreasing).
        """
        if ts.length() <= 1:
            return

        if pc.any(pc.less(ts.slice(1), ts.slice(0, ts.length() - 1))).as_py():
            raise ValueError(
                "MinuteTradeAggEngine requires input sorted by ts"
            )
