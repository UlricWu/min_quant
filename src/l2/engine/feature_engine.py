#!filepath: src/l2/offline/feature_engine.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Optional

from .events import OrderBookSnapshot, EnrichedTradeEvent


@dataclass
class FeatureEngineConfig:
    compute_mid: bool = True
    compute_spread: bool = True
    compute_imbalance: bool = True


class FeatureEngine:
    """
    Streaming 特征引擎：
    - from_snapshot：基于订单簿快照提取结构特征
    - from_trade   ：基于成交事件提取成交特征
    """

    def __init__(self, cfg: FeatureEngineConfig | None = None) -> None:
        self.cfg = cfg or FeatureEngineConfig()

    # 订单簿特征
    def from_snapshot(self, snap: OrderBookSnapshot) -> Dict[str, Optional[float]]:
        feats: Dict[str, Optional[float]] = {}

        bids = snap.bids
        asks = snap.asks

        if self.cfg.compute_mid and bids and asks:
            best_bid = bids[0][0]
            best_ask = asks[0][0]
            feats["mid"] = (best_bid + best_ask) / 2
        else:
            feats["mid"] = None

        if self.cfg.compute_spread and bids and asks:
            feats["spread"] = asks[0][0] - bids[0][0]
        else:
            feats["spread"] = None

        if self.cfg.compute_imbalance and bids and asks:
            bid_vol = bids[0][1]
            ask_vol = asks[0][1]
            denom = bid_vol + ask_vol
            feats["imbalance"] = (bid_vol - ask_vol) / denom if denom != 0 else None
        else:
            feats["imbalance"] = None

        return feats

    # 成交特征
    def from_trade(self, ev: EnrichedTradeEvent) -> Dict[str, float | int]:
        return {
            "price": ev.price,
            "qty": ev.qty,
            "burst_id": ev.burst_id,
            "is_price_impact": int(ev.is_price_impact),
        }
