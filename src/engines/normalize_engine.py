#!filepath: src/engines/normalize_engine.py
from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from src.engines.context import EngineContext
# from src.l2.common.normalized_event import NormalizedEvent
from src.l2.common.event_parser import parse_events, EventKind
from src import logs

from dataclasses import dataclass, asdict


@dataclass(slots=True)
class NormalizedEvent:
    symbol: str
    ts: int
    event: str
    order_id: int
    side: str | None
    price: float
    volume: int
    buy_no: int
    sell_no: int

    @classmethod
    def from_row(cls, row):
        return cls(
            symbol=str(row.symbol),
            ts=int(row.ts),
            event=row.event,
            order_id=int(row.order_id),
            side=None if row.side != row.side else row.side,
            price=float(row.price),
            volume=int(row.volume),
            buy_no=int(row.buy_no),
            sell_no=int(row.sell_no),
        )

    def to_dict(self) -> dict:
        return asdict(self)


class NormalizeEngine:
    """
    NormalizeEngine（正确契约版）

    - 统一 schema（symbol / ts 真相）
    - ts = int（微秒）
    - order / trade 分文件输出
    - 排序
    - 不泄漏 vendor 字段
    """

    VALID_EVENTS = {"ADD", "CANCEL", "TRADE"}

    # ===========================
    # Public Entry
    # ===========================
    def execute(self, ctx: EngineContext) -> None:
        assert ctx.mode == "offline"
        assert ctx.input_path is not None
        assert ctx.output_path is not None
        assert ctx.symbol is not None

        symbol = self._normalize_symbol(ctx.symbol)

        order_path = ctx.input_path / "Order.parquet"
        trade_path = ctx.input_path / "Trade.parquet"

        if order_path.exists():
            self._normalize_file(
                path=order_path,
                kind="order",
                symbol=symbol,
                out_dir=ctx.output_path / "order",
            )

        if trade_path.exists():
            self._normalize_file(
                path=trade_path,
                kind="trade",
                symbol=symbol,
                out_dir=ctx.output_path / "trade",
            )

    # ===========================
    # Core normalize
    # ===========================
    def _normalize_file(
        self,
        *,
        path: Path,
        kind: EventKind,
        symbol: str,
        out_dir: Path,
    ) -> None:
        df = pd.read_parquet(path)
        if df.empty:
            return

        norm_df = parse_events(df, kind=kind)
        if norm_df.empty:
            return

        # 合法事件裁决
        norm_df = norm_df[norm_df["event"].isin(self.VALID_EVENTS)]
        if norm_df.empty:
            return

        # 注入 symbol（Normalize 的职责）
        norm_df = norm_df.copy()
        norm_df["symbol"] = symbol

        # 排序（Normalize 的职责）
        norm_df.sort_values("ts", inplace=True)

        events = [NormalizedEvent.from_row(r) for r in norm_df.itertuples(index=False)]
        self._write(events, out_dir)

    # ===========================
    # Write parquet (schema lock)
    # ===========================
    def _write(self, events: List[NormalizedEvent], out_dir: Path) -> None:
        if not events:
            return

        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "Normalized.parquet"

        rows = [e.to_dict() for e in events]
        df = pd.DataFrame(rows)

        expected_cols = [
            "symbol",
            "ts",
            "event",
            "order_id",
            "side",
            "price",
            "volume",
            "buy_no",
            "sell_no",
        ]
        df = df[expected_cols]

        if not pd.api.types.is_integer_dtype(df["ts"]):
            raise TypeError("[NormalizeEngine] ts must be int")

        df.to_parquet(out_path, index=False)
        logs.info(f"[Normalize] wrote {len(df)} → {out_path}")

    @staticmethod
    def _normalize_symbol(symbol: str | int) -> str:
        return str(symbol).strip().zfill(6)
