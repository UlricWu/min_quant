# src/data/l2/feature_builder.py
from typing import List, Dict
import pandas as pd
from src.l2.orderbook_state import OrderBookState


class OrderBookFeatureBuilder:
    def __init__(self, max_levels: int = 10) -> None:
        self.max_levels = max_levels

    def build_features(self, orderbook: OrderBookState) -> Dict:
        best_bid = orderbook.best_bid
        best_ask = orderbook.best_ask
        spread = best_ask.price - best_bid.price if best_bid and best_ask else None
        mid_price = (best_bid.price + best_ask.price) / 2 if best_bid and best_ask else None

        # Depth features
        bid_depth_1_10 = sum(level.volume for level in orderbook.bids[:10])
        ask_depth_1_10 = sum(level.volume for level in orderbook.asks[:10])

        micro_price = None
        if bid_depth_1_10 + ask_depth_1_10 > 0:
            micro_price = (best_bid.price * bid_depth_1_10 + best_ask.price * ask_depth_1_10) / (bid_depth_1_10 + ask_depth_1_10)

        return {
            "mid_price": mid_price,
            "spread": spread,
            "best_bid_price": best_bid.price if best_bid else None,
            "best_bid_volume": best_bid.volume if best_bid else None,
            "best_ask_price": best_ask.price if best_ask else None,
            "best_ask_volume": best_ask.volume if best_ask else None,
            "bid_depth_1_10": bid_depth_1_10,
            "ask_depth_1_10": ask_depth_1_10,
            "micro_price": micro_price,
        }

    def to_dataframe(self, features: List[Dict]) -> pd.DataFrame:
        return pd.DataFrame(features)
