# src/l2/orderbook_state.py
from typing import List, Optional
from dataclasses import dataclass
from event_types import SnapshotEvent, OrderBookUpdateEvent

@dataclass
class BookSideLevel:
    price: float
    volume: int


@dataclass
class OrderBookState:
    max_levels: int = 10
    bids: List[BookSideLevel] = []
    asks: List[BookSideLevel] = []
    last_price: Optional[float] = None
    last_volume: Optional[int] = None
    last_amount: Optional[float] = None
    timestamp_ns: Optional[int] = None

    def update_from_snapshot(self, snapshot: SnapshotEvent) -> None:
        self.timestamp_ns = snapshot.timestamp_ns
        self.bids = [
            BookSideLevel(price, volume)
            for price, volume in zip(snapshot.bid_prices[:self.max_levels], snapshot.bid_volumes[:self.max_levels])
        ]
        self.asks = [
            BookSideLevel(price, volume)
            for price, volume in zip(snapshot.ask_prices[:self.max_levels], snapshot.ask_volumes[:self.max_levels])
        ]
        self.last_price = snapshot.last_price

    def update_from_order(self, order: OrderBookUpdateEvent) -> None:
        self.timestamp_ns = order.timestamp_ns
        levels = self.bids if order.side == "B" else self.asks
        idx = order.level - 1
        while len(levels) <= idx:
            levels.append(BookSideLevel(price=0.0, volume=0))
        levels[idx].price = order.price
        levels[idx].volume = order.volume
        if order.side == "B":
            levels.sort(key=lambda lv: lv.price, reverse=True)
        else:
            levels.sort(key=lambda lv: lv.price)

    def update_last_trade(self, price: float, volume: int, amount: float, timestamp_ns: int) -> None:
        self.last_price = price
        self.last_volume = volume
        self.last_amount = amount
        self.timestamp_ns = timestamp_ns

    @property
    def best_bid(self) -> Optional[BookSideLevel]:
        if not self.bids:
            return None
        return self.bids[0]

    @property
    def best_ask(self) -> Optional[BookSideLevel]:
        if not self.asks:
            return None
        return self.asks[0]
