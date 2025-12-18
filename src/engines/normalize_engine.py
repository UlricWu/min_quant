#!filepath: src/engines/normalize_engine.py
from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd

from src.engines.context import EngineContext
from src.l2.common.normalized_event import NormalizedEvent
from src.l2.common.event_parser import parse_events, EventKind
from src import logs


class NormalizeEngine:
    """
    NormalizeEngine（Offline + Realtime 共用，最终版）

    职责（红线）：
    - 逐笔 / 委托 / 成交 → NormalizedEvent
    - ts 在此 Engine 内被“钉死”为 int（唯一真相）
    - 负责时间排序
    - 负责 parquet 输出（offline）

    下游 Engine：
    - 不允许再看到 Timestamp / datetime
    """

    # ==================================================
    # 唯一 public 入口（Adapter 只调用这个）
    # ==================================================
    def execute(self, ctx: EngineContext) -> List[NormalizedEvent]:
        """
        返回 NormalizedEvent 列表

        - offline :
            * 从 Order.parquet / Trade.parquet 读取
            * normalize
            * 排序
            * 写 Events.parquet
            * 返回 events

        - realtime / replay :
            * ctx.event != None
            * 返回 [NormalizedEvent]
        """
        if ctx.mode == "offline":
            return self._run_offline(ctx)

        # realtime / replay
        if ctx.event is None:
            raise ValueError("[NormalizeEngine] ctx.event is required in realtime mode")

        return [self._normalize_one(ctx.event)]

    # ==================================================
    # Offline：目录 → Events.parquet
    # ==================================================
    def _run_offline(self, ctx: EngineContext) -> List[NormalizedEvent]:
        assert ctx.input_path is not None
        assert ctx.output_path is not None

        events: List[NormalizedEvent] = []

        order_path = ctx.input_path / "Order.parquet"
        trade_path = ctx.input_path / "Trade.parquet"

        if order_path.exists():
            events.extend(self._normalize_file(order_path, kind="order"))

        if trade_path.exists():
            events.extend(self._normalize_file(trade_path, kind="trade"))

        if not events:
            logs.warning(
                f"[NormalizeEngine] no events "
                f"symbol={ctx.symbol} date={ctx.date}"
            )
            return []

        # NormalizeEngine 的责任：时间排序
        events.sort(key=lambda e: e.ts)

        self._write_parquet(events, ctx.output_path)
        return events

    # ==================================================
    # parquet → NormalizedEvent[]
    # ==================================================
    def _normalize_file(self, path: Path, *, kind: EventKind) -> List[NormalizedEvent]:
        df = pd.read_parquet(path)
        if df.empty:
            return []

        # SH / SZ / Order / Trade → 统一 schema DataFrame
        # 🔒 核心立法：只允许合法事件
        norm_df = parse_events(df, kind=kind)
        if norm_df.empty:
            return []

        # 🔒 核心立法：只允许合法事件
        before = len(norm_df)
        norm_df = norm_df[norm_df["event"].isin(["ADD", "CANCEL", "TRADE"])]

        dropped = before - len(norm_df)
        if dropped > 0:
            logs.debug(
                f"[NormalizeEngine] drop {dropped} invalid events "
                f"({kind}, path={path.name})"
            )

        events: List[NormalizedEvent] = []

        # ⚠️ 这里假定：
        # - parse_events 已经输出 ts = int
        # - NormalizeEngine 之后不允许 Timestamp
        for row in norm_df.itertuples(index=False):
            events.append(NormalizedEvent.from_row(row))

        return events

    # ==================================================
    # Realtime：单条事件 → NormalizedEvent
    # ==================================================
    def _normalize_one(self, raw_event) -> NormalizedEvent:
        """
        raw_event:
            - dict
            - namedtuple
            - vendor event

        返回：
            - NormalizedEvent（ts = int）
        """
        df = pd.DataFrame([raw_event])
        norm_df = parse_events(df, kind='trade')

        if norm_df.empty:
            raise ValueError("[NormalizeEngine] normalize_one got empty result")

        row = norm_df.iloc[0]
        return NormalizedEvent.from_row(row)

    # ==================================================
    # 写 parquet（Engine 内部细节）
    # ==================================================
    def _write_parquet(
            self,
            events: List[NormalizedEvent],
            output_path: Path,
    ) -> None:
        output_path.parent.mkdir(parents=True, exist_ok=True)

        rows = [e.to_dict() for e in events]
        df = pd.DataFrame(rows)

        # 再次兜底校验（防 ts 泄漏）
        if not pd.api.types.is_integer_dtype(df["ts"]):
            raise TypeError("[NormalizeEngine] ts must be int before writing parquet")

        df.to_parquet(output_path, index=False)

        logs.info(
            f"[NormalizeEngine] wrote {len(df)} events → {output_path}"
        )
