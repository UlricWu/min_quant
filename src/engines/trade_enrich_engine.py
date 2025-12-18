from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Iterator
from src.engines.context import EngineContext
from src.l2.common.normalized_event import NormalizedEvent
from src import logs
import pandas as pd

# =========================
# 输入 / 输出事件定义
# =========================
@dataclass(frozen=True)
class RawTradeEvent:
    ts: datetime
    price: float
    volume: int
    side: str | None   # 'B' / 'S' / None


@dataclass(frozen=True)
class EnrichedTradeEvent:
    ts: datetime
    price: float
    volume: int
    side: str | None
    notional: float
    signed_volume: int


# =========================
# Engine
# =========================
class TradeEnrichEngine:
    """
    Trade Enrich 引擎（Offline + Realtime 共用）

    职责：
    - 计算 notional
    - 计算 signed_volume
    - 不处理交易所差异
    - 不解析时间
    """


    def __init__(self):
        self._rows = []

    # ======================================================
    # 唯一入口
    # ======================================================
    def execute(self, ctx: EngineContext) -> None:
        if ctx.mode == "offline":
            assert ctx.input_path and ctx.output_path
            self._run_offline(ctx)

        else:
            assert ctx.event
            self._apply(ctx.event)

    # ======================================================
    # Offline
    # ======================================================
    def _run_offline(self, ctx: EngineContext) -> None:
        logs.info(f"[TradeEnrich] symbol={ctx.symbol} date={ctx.date}")

        df = pd.read_parquet(ctx.input_path)
        for ev in self._iter_events(df):
            self._apply(ev)

        out_df = pd.DataFrame(self._rows)
        out_df.to_parquet(ctx.output_path, index=False)

    # ======================================================
    # 核心逻辑
    # ======================================================
    def _apply(self, ev: NormalizedEvent) -> None:
        if ev.event != "TRADE":
            return

        self._rows.append(
            {
                "ts_ns": ev.ts,
                "price": ev.price,
                "volume": ev.volume,
                "side": ev.side,
                "notional": ev.price * ev.volume,
                "signed_volume": ev.volume if ev.side == "B" else -ev.volume,
            }
        )

    # ======================================================
    @staticmethod
    def _iter_events(df: pd.DataFrame) -> Iterable[NormalizedEvent]:
        for row in df.itertuples(index=False):
            yield NormalizedEvent.from_row(row)