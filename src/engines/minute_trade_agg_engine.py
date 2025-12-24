# #!filepath: src/engines/minute_trade_agg_engine.py
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

US_PER_MINUTE = 60 * 1_000_000


class MinuteTradeAggEngine:
    """
    MinuteTradeAggEngine（冻结版）

    输入：
      - enriched trade Arrow Table

    输出：
      - minute trade Arrow Table（OHLCV + amount）

        Input (Trade_Enriched.parquet) columns (minimum):
      - ts (int64, us)
      - price (float)
      - volume (int)
      - notional (float)

    Output (Minute_Trade.parquet):
      - minute_ts
      - open, high, low, close
      - volume, signed_volume, notional
      - trade_count (optional)

    设计原则：
      - 纯计算
      - 无状态
      - 不做 IO
      - 不关心 Meta / Pipeline

    Cross-day semantics (Frozen):

- MinuteTradeAggEngine performs pure time-based aggregation.
- It does NOT infer trading days or session boundaries.
- It does NOT fill missing minutes.
- Cross-day gaps (e.g. 23:59 → 09:30) are preserved as-is.
- Trading calendar semantics must be handled upstream.
MinuteTradeAggEngine outputs naive datetime minute buckets.
Timezone semantics are intentionally excluded and must be handled upstream.


    """

    def __init__(self):
        pass

    # --------------------------------------------------
    def execute(self, table: pa.Table) -> pa.Table:
        """
        聚合 enriched trade → minute trade

        Parameters
        ----------
        table : pa.Table
            enriched trade table（必须包含 ts / price / volume / symbol）

        Returns
        -------
        pa.Table
            minute trade fact table
        """
        if table.num_rows == 0:
            return table

        self._assert_sorted_ts(table["ts"])

        # --------------------------------------------------
        # 1. 生成 minute bucket
        # --------------------------------------------------
        # --------------------------------------------------
        # 1. minute bucket (int64)
        # --------------------------------------------------
        minute_id = pc.cast(
            pc.divide(table["ts"], pa.scalar(US_PER_MINUTE)),
            pa.int64()
        )

        table = table.append_column("minute_id", minute_id)

        # --------------------------------------------------
        # ★ 1.5 修正 signed_volume 的 NULL
        # --------------------------------------------------
        # signed_volume = pc.fill_null(
        #     table["signed_volume"],
        #     pa.scalar(0, pa.int64()),
        # )
        # table = table.set_column(
        #     table.schema.get_field_index("signed_volume"),
        #     "signed_volume",
        #     signed_volume,
        # )

        # --------------------------------------------------
        # 2. group by minute_id
        # --------------------------------------------------
        grouped = (
            table
            .group_by("minute_id", use_threads=False)  # ← 关键！必须写
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
        # 4. minute_id -> timestamp[ns]
        # --------------------------------------------------
        minute_ts = pc.multiply(
            grouped["minute_id"],
            pa.scalar(US_PER_MINUTE),
        )
        minute_ts = pc.cast(minute_ts, pa.timestamp("us"))

        grouped = (
            grouped
            .append_column("minute", minute_ts)
            .drop(["minute_id"])
        )

        # --------------------------------------------------
        # --------------------------------------------------
        # 4. column normalization (关键一步)
        # --------------------------------------------------
        cols = {
            "minute": minute_ts,
            "open": grouped["price_first"],
            "high": grouped["price_max"],
            "low": grouped["price_min"],
            "close": grouped["price_last"],
            "volume": grouped["volume_sum"],
            "notional": grouped["notional_sum"],
        }

        # if self.cfg.include_trade_count:
        cols["trade_count"] = grouped["price_count"]

        return pa.table(cols)

    def _assert_sorted_ts(self, ts: pa.Array) -> None:
        if ts.length() <= 1:
            return
        if pc.any(pc.less(ts.slice(1), ts.slice(0, ts.length() - 1))).as_py():
            raise ValueError("MinuteTradeAggEngine requires input sorted by ts")
