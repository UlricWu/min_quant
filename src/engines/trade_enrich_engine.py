#!filepath: src/engines/trade_enrich_engine.py

from __future__ import annotations
import pyarrow as pa
import pyarrow.compute as pc
from typing import Optional, Dict, Any

from src import logs


class TradeEnrichEngine:
    """
    纯逻辑 Enrich Engine（可用于 offline / realtime / 回测）。

    输入：RecordBatch (必须包含 ts_ns / Price / Volume / Side)
    输出：RecordBatch（增加 burst_id / is_impact / impact_bp 等字段）
    """

    def __init__(self, burst_window_ms: int = 5):
        # self.burst_window_ns = burst_window_ms * 1_000_000
        self.burst_window_ns = 5000000

    # ------------------------------------------------------------------
    def enrich_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """
        enrich 单个 Arrow batch，不负责跨 batch 状态保存。
        上层 step 会保证 batch 顺序。
        """
        if batch.num_rows == 0:
            return batch

        cols = batch.to_pydict()

        ts = cols.get("ts")
        price = cols.get("price")
        volume = cols.get("volume")
        side = cols.get("side")

        if ts is None:
            raise KeyError("TradeEnrichEngine 需要列 ts_ns")
        if price is None:
            raise KeyError("TradeEnrichEngine 需要列 price")

        n = len(ts)

        burst_id = [0] * n
        is_impact = [False] * n
        impact_bp = [0.0] * n

        last_ts = None
        last_price = None
        last_burst = 0

        for i in range(n):
            cur_ts = ts[i]
            cur_price = price[i]

            # -------------------------------
            # Burst（成交簇）检测
            # -------------------------------
            if last_ts is None:
                burst_id[i] = 0
            else:
                if cur_ts - last_ts > self.burst_window_ns:
                    last_burst += 1
                burst_id[i] = last_burst

            # -------------------------------
            # Price Impact（简单版本）
            # -------------------------------
            if last_price is not None:
                diff = cur_price - last_price
                impact_bp[i] = (diff / last_price) * 10000.0
                is_impact[i] = abs(impact_bp[i]) > 1  # 绝对阈值可配置

            last_ts = cur_ts
            last_price = cur_price

        # -------------------------------
        # 合成为新的 RecordBatch
        # -------------------------------
        new_cols = dict(cols)
        new_cols["burst_id"] = burst_id
        new_cols["is_impact"] = is_impact
        new_cols["impact_bp"] = impact_bp

        # Arrow 自动推断类型
        arrays = {k: pa.array(v) for k, v in new_cols.items()}
        out_batch = pa.record_batch(arrays)

        return out_batch
