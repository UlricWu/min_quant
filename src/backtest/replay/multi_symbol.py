# src/backtest/replay/multi_symbol.py
from __future__ import annotations

import heapq
from typing import Dict, Iterator, List, Tuple

from src.backtest.events import MarketEvent
from src import logs


class MultiSymbolReplay:
    """
    MultiSymbolReplay (FINAL / FROZEN)

    职责：
      - 多 symbol MarketEvent 流合并（k-way merge）
      - 保证全局 ts 单调不回退
      - 不修改 event
      - 不持久化状态

    输入假设（冻结）：
      - 每个 iterator 内部 ts 单调递增
      - iterator 是惰性的（generator）
      Replay Ordering Semantics (Frozen)

Events are ordered primarily by ts (ascending).

If multiple events share the same ts, they are ordered by a monotonically increasing replay sequence id.

The sequence id is assigned by the replay engine at insertion time.

Replay ordering must be deterministic and reproducible.

Replay must never reorder events based on symbol, exchange, or any business attribute.
    """

    def __init__(self, streams: Dict[str, Iterator[MarketEvent]]):
        """
        streams:
          symbol -> MarketEvent iterator
        """
        if not streams:
            raise ValueError("[MultiSymbolReplay] empty streams")

        self._streams = streams

    # --------------------------------------------------
    def replay(self) -> Iterator[MarketEvent]:
        """
        K-way merge of sorted MarketEvent streams.
        """
        # heap item:
        # (ts, seq, symbol, event, iterator)
        heap: List[
            Tuple[int, int, str, MarketEvent, Iterator[MarketEvent]]
        ] = []

        seq = 0          # ts 相等时的稳定排序
        last_ts = None   # 全局时间断言

        # --------------------------------------------------
        # 初始化：每个 symbol 取第一个 event
        # --------------------------------------------------
        for symbol, it in self._streams.items():
            try:
                ev = next(it)
            except StopIteration:
                logs.warning(
                    f"[MultiSymbolReplay] empty event stream: {symbol}"
                )
                continue

            heap.append((ev.ts, seq, symbol, ev, it))
            seq += 1

        heapq.heapify(heap)

        # --------------------------------------------------
        # 主循环：不断弹出最小 ts
        # --------------------------------------------------
        while heap:
            ts, _, symbol, ev, it = heapq.heappop(heap)

            # 🔒 全局时间语义断言（不可修复）
            if last_ts is not None and ts < last_ts:
                raise RuntimeError(
                    "[MultiSymbolReplay] global ts regression: "
                    f"{ts} < {last_ts} (symbol={symbol})"
                )

            last_ts = ts
            yield ev

            # 推进该 symbol 的 stream
            try:
                nxt = next(it)
            except StopIteration:
                continue

            heapq.heappush(
                heap,
                (nxt.ts, seq, symbol, nxt, it),
            )
            seq += 1
