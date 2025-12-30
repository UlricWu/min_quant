from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.pipeline.context import EngineContext
from src.utils.logger import logs

US_PER_MINUTE = 60 * 1_000_000


@dataclass(frozen=True)
class MinuteOrderAggConfig:
    include_order_count: bool = True


class MinuteOrderAggEngine:
    """
    MinuteOrderAggEngine（基于 OrderBookRebuild 输出）

    Input:
      orderbook_events.parquet
        - ts
        - event: ADD / CANCEL / TRADE
        - volume
        - signed_volume
        - notional

    Output:
        minute_order.parquet
          - minute           timestamp[us]
          - add_volume       int64
          - cancel_volume    int64
          - net_volume       int64
          - add_notional     float64
          - cancel_notional  float64
          - order_count      int64   (optional)

    """

    def __init__(self, cfg: Optional[MinuteOrderAggConfig] = None) -> None:
        self.cfg = cfg or MinuteOrderAggConfig()

    # ======================================================
    def execute(self, ctx: EngineContext) -> None:
        if ctx.mode != "offline":
            raise NotImplementedError("MinuteOrderAggEngine only supports offline mode")

        assert ctx.input_file and ctx.output_file
        self._run_offline(ctx)

    # ======================================================
    def _run_offline(self, ctx: EngineContext) -> None:
        table = pq.read_table(ctx.input_file)

        if table.num_rows == 0:
            # logs.warning("[MinuteOrderAgg] empty input")
            # pq.write_table(table, ctx.output_path)
            return


        # --------------------------------------------------
        # 1. minute bucket
        # --------------------------------------------------
        ts_us = pc.cast(table["ts"], pa.int64())
        US_PER_MINUTE = 60 * 1_000_000

        minute_id = pc.divide(ts_us, pa.scalar(US_PER_MINUTE))

        # minute_id = pc.cast(
        #     pc.divide(table["ts"], pa.scalar(US_PER_MINUTE)),
        #     pa.int64(),
        # )
        table = table.append_column("minute_id", minute_id)

        # --------------------------------------------------
        # 2. 只保留 ADD / CANCEL
        # --------------------------------------------------
        is_add = pc.equal(table["event"], pa.scalar("ADD"))
        is_cancel = pc.equal(table["event"], pa.scalar("CANCEL"))

        add_volume = pc.if_else(is_add, table["volume"], pa.scalar(0))
        cancel_volume = pc.if_else(is_cancel, table["volume"], pa.scalar(0))

        add_notional = pc.if_else(is_add, table["notional"], pa.scalar(0.0))
        cancel_notional = pc.if_else(is_cancel, table["notional"], pa.scalar(0.0))

        table = (
            table
            .append_column("add_volume", add_volume)
            .append_column("cancel_volume", cancel_volume)
            .append_column("add_notional", add_notional)
            .append_column("cancel_notional", cancel_notional)
        )

        # --------------------------------------------------
        # 3. group by minute_id
        # --------------------------------------------------
        aggs = [
            ("add_volume", "sum"),
            ("cancel_volume", "sum"),
            ("add_notional", "sum"),
            ("cancel_notional", "sum"),
        ]

        if self.cfg.include_order_count:
            aggs.append(("event", "count"))

        grouped = (
            table
            .group_by("minute_id", use_threads=False)
            .aggregate(aggs)
        )

        # --------------------------------------------------
        # 4. 派生字段
        # --------------------------------------------------
        net_volume = pc.subtract(
            grouped["add_volume_sum"],
            grouped["cancel_volume_sum"],
        )


        minute_ts = pc.cast(
            pc.multiply(grouped["minute_id"], pa.scalar(US_PER_MINUTE)),
            pa.timestamp("us"),
        )

        grouped = (
            grouped
            .append_column("net_volume", net_volume)
            .append_column("minute", minute_ts)
            .drop(["minute_id"])
        )

        cols = {
            "minute": minute_ts,
            "add_volume": grouped["add_volume_sum"],
            "cancel_volume": grouped["cancel_volume_sum"],
            "net_volume": net_volume,
            "add_notional": grouped["add_notional_sum"],
            "cancel_notional": grouped["cancel_notional_sum"],
        }

        # if self.cfg.include_order_count:
        cols["event_count"] = grouped["event_count"]

        result = pa.table(cols)

        pq.write_table(result, ctx.output_file)
