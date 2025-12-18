from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional
import pandas as pd
from datetime import datetime
from src.engines.context import EngineContext
from src.l2.common.normalized_event import NormalizedEvent
from src import logs
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
    Trade Enrich Engine（严格 trade-only）

    ✔ offline / realtime 共用
    ✔ 不依赖盘口 / snapshot / orderbook
    ✔ 不推断 aggressor
    ✔ 不计算 VPIN / impact
    """

    def __init__(self, config: Optional[TradeEnrichConfig] = None) -> None:
        self.cfg = config or TradeEnrichConfig()

        # realtime / offline 共用状态
        self._rows: list[dict] = []
        self._last_ts_ms: Optional[int] = None
        self._burst_id: int = 0

        # offline 才会用到
        self._volume_series: list[int] = []
        self._small_thr: Optional[float] = None
        self._large_thr: Optional[float] = None

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
        logs.info(f"[TradeEnrich] trade-only symbol={ctx.symbol} date={ctx.date}")

        df = pd.read_parquet(ctx.input_path)

        # ① 只保留 TRADE
        trade_df = df[df["event"] == "TRADE"].copy()
        if trade_df.empty:
            logs.warning("[TradeEnrich] no TRADE events")
            pd.DataFrame().to_parquet(ctx.output_path, index=False)
            return

        # ② 预计算分位数（trade_bucket 用）
        self._prepare_volume_buckets(trade_df["volume"])

        # ③ 顺序处理
        for ev in self._iter_events(trade_df):
            self._apply(ev)

        out_df = pd.DataFrame(self._rows)
        out_df.to_parquet(ctx.output_path, index=False)

    # ======================================================
    # 核心逻辑（trade-only）
    # ======================================================
    def _apply(self, ev: NormalizedEvent) -> None:
        if ev.event != "TRADE":
            return

        ts_ms = int(ev.ts) // 1_000_000

        # ---------- burst ----------
        if self._last_ts_ms is not None:
            if ts_ms - self._last_ts_ms > self.cfg.burst_window_ms:
                self._burst_id += 1
        self._last_ts_ms = ts_ms

        # ---------- signed volume ----------
        if ev.side == "B":
            signed_volume = ev.volume
        elif ev.side == "S":
            signed_volume = -ev.volume
        else:
            signed_volume = 0

        # ---------- trade bucket ----------
        bucket = self._bucket_trade_size(ev.volume)

        self._rows.append(
            {
                "ts": ev.ts,
                "price": ev.price,
                "volume": ev.volume,
                "side": ev.side,
                "notional": ev.price * ev.volume,
                "signed_volume": signed_volume,
                "trade_bucket": bucket,
                "burst_id": self._burst_id,
            }
        )

    # ======================================================
    # Helpers
    # ======================================================
    def _prepare_volume_buckets(self, volume: pd.Series) -> None:
        self._small_thr = volume.quantile(self.cfg.medium_trade_pct)
        self._large_thr = volume.quantile(self.cfg.large_trade_pct)

    def _bucket_trade_size(self, v: int) -> str:
        if self._large_thr is None or self._small_thr is None:
            return "U"  # Unknown（realtime / 未初始化）

        if v >= self._large_thr:
            return "L"
        elif v >= self._small_thr:
            return "M"
        return "S"

    @staticmethod
    def _iter_events(df: pd.DataFrame) -> Iterable[NormalizedEvent]:
        for row in df.itertuples(index=False):
            yield NormalizedEvent.from_row(row)
