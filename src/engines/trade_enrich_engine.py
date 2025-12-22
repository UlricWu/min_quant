#!filepath: src/engines/trade_enrich_engine.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from src.pipeline.context import EngineContext


# =========================
# 输入 / 输出事件定义
# =========================
@dataclass(frozen=True)
class RawTradeEvent:
    ts: datetime
    price: float
    volume: int
    side: str | None  # 'B' / 'S' / None


@dataclass(frozen=True)
class EnrichedTradeEvent:
    ts: datetime
    price: float
    volume: int
    side: str | None
    notional: float
    signed_volume: int


@dataclass
class TradeEnrichConfig:
    """
    Trade-only 增强配置
    """
    burst_window_ms: int = 30
    large_trade_pct: float = 0.90
    medium_trade_pct: float = 0.50


class TradeEnrichEngine:
    """
    TradeEnrichEngine (Arrow-only, FINAL)

    Assumptions (FROZEN):
    - input parquet is trade-only (already normalized)
    - columns exist: ts, price, volume, side
    - v1: materialize trade.parquet
    """

    # --------------------------------------------------
    def execute(self, ctx: EngineContext) -> None:
        assert ctx.mode == "offline"
        assert ctx.input_path and ctx.output_path

        self._run_offline(ctx.input_path, ctx.output_path)

    # --------------------------------------------------
    def _run_offline(self, input_path, output_path) -> None:
        # ① 只读必要列（真裁剪）
        table = pq.read_table(
            input_path,
            columns=["ts", "price", "volume", "side"],
        )

        if table.num_rows == 0:
            pq.write_table(table, output_path)
            return

        price = table["price"]
        volume = table["volume"]
        side = table["side"]

        # ② notional = price * volume
        notional = pc.multiply(price, volume)

        # ③ signed_volume
        # side == 'B' → +volume
        # else         → -volume
        signed_volume = pc.multiply(
            volume,
            pc.if_else(
                pc.equal(side, pa.scalar("B")),
                pa.scalar(1, pa.int8()),
                pa.scalar(-1, pa.int8()),
            ),
        )

        # ④ 组装输出 Table（无 pandas）
        out_table = pa.table(
            {
                "ts": table["ts"],
                "price": price,
                "volume": volume,
                "side": side,
                "notional": notional,
                "signed_volume": signed_volume,
            }
        )

        # ⑤ 写 parquet（一次）
        pq.write_table(out_table, output_path)
