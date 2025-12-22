#!filepath: src/engines/minute_trade_agg_engine.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.pipeline.context import EngineContext
from src.utils.logger import logs


NS_PER_MINUTE = 60 * 1_000_000_000


@dataclass(frozen=True)
class MinuteTradeAggConfig:
    # 你可以之后扩展：是否输出 trade_count、是否保留 side 等
    include_trade_count: bool = True


class MinuteTradeAggEngine:
    """
    MinuteTradeAggEngine（Arrow-only）

    Input (Trade_Enriched.parquet) columns (minimum):
      - ts (int64, ns)
      - price (float)
      - volume (int)
      - signed_volume (int)
      - notional (float)

    Output (Minute_Trade.parquet):
      - minute_ts
      - open, high, low, close
      - volume, signed_volume, notional
      - trade_count (optional)
    """

    def __init__(self, cfg: Optional[MinuteTradeAggConfig] = None) -> None:
        self.cfg = cfg or MinuteTradeAggConfig()

    # ======================================================
    # 唯一入口
    # ======================================================
    def execute(self, ctx: EngineContext) -> None:
        if ctx.mode != "offline":
            raise NotImplementedError("MinuteTradeAggEngine 目前只做 offline batch")

        assert ctx.input_path and ctx.output_path
        self._run_offline(ctx)

    # ======================================================
    # Offline
    # ======================================================
    def _run_offline(self,ctx: EngineContext) -> None:
        table = pq.read_table(ctx.input_path)

        if table.num_rows == 0:
            logs.warning("[MinuteTradeAgg] empty input")
            pq.write_table(table, ctx.output_path)
            return

        # --------------------------------------------------
        # 1. minute bucket (int64)
        # --------------------------------------------------
        minute_id = pc.cast(
            pc.divide(table["ts"], pa.scalar(NS_PER_MINUTE)),
            pa.int64()
        )

        table = table.append_column("minute_id", minute_id)

        # --------------------------------------------------
        # 2. group by minute_id
        # --------------------------------------------------
        grouped = (
            table
            .group_by("minute_id", use_threads=False) # ← 关键！必须写
            .aggregate(
                [
                    ("price", "first"),  # open
                    ("price", "max"),  # high
                    ("price", "min"),  # low
                    ("price", "last"),  # close
                    ("volume", "sum"),
                    ("signed_volume", "sum"),
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
            pa.scalar(NS_PER_MINUTE),
        )
        minute_ts = pc.cast(minute_ts, pa.timestamp("ns"))

        grouped = (
            grouped
            .append_column("minute", minute_ts)
            .drop(["minute_id"])
        )

        # --------------------------------------------------
        pq.write_table(grouped, ctx.output_path)
        # logs.info(f"[MinuteTradeAgg] written → {ctx.output_path}")