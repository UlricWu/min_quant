# #!filepath: src/l2/orderbook/orderbook_store.py
# from __future__ import annotations
# from collections import defaultdict
# from dataclasses import dataclass, field
# from typing import Dict, List, Literal, Tuple
# import bisect
#
# Side = Literal["B", "S"]
#
#
# @dataclass
# class OrderBook:
#     """
#     订单簿 OrderBook（适配 L2）
#
#     使用两个结构：
#     1. price → volume（字典）
#     2. sorted price list（买：降序, 卖：升序）
#
#     复杂度：
#     - 插入/更新价格档位： O(1)
#     - 插入价格档位到列表： O(logN)
#     - 获取 L1-L10 快照： O(10) ≈ O(1)
#
#     这是真正可用于生产的订单簿结构。
#     """
#
#     bids: Dict[float, int] = field(default_factory=lambda: defaultdict(int))
#     asks: Dict[float, int] = field(default_factory=lambda: defaultdict(int))
#
#     bid_prices: List[float] = field(default_factory=list)
#     ask_prices: List[float] = field(default_factory=list)
#
#     level = 10
#
#     # --------------------------
#     # 内部工具方法
#     # --------------------------
#
#     def _insert_price(self, price_list: List[float], price: float, reverse: bool):
#         """在排序列表中插入一个新的价格档位。"""
#         if price in price_list:
#             return
#         # 买档为降序，卖档为升序
#         if reverse:
#             # bisect 只能升序，因此插入负值
#             pos = bisect.bisect_left(price_list, price)
#             price_list.insert(pos, price)
#         else:
#             pos = bisect.bisect_left(price_list, price)
#             price_list.insert(pos, price)
#
#     # --------------------------
#     # 核心操作：新增委托
#     # --------------------------
#     def add_order(self, side: Side, price: float, volume: int):
#         if volume <= 0:
#             return
#
#         if side == "B":
#             if price not in self.bids:
#                 self._insert_price(self.bid_prices, price, reverse=True)
#             self.bids[price] += volume
#         else:
#             if price not in self.asks:
#                 self._insert_price(self.ask_prices, price, reverse=False)
#             self.asks[price] += volume
#
#     # --------------------------
#     # 撤单（部分 or 全撤）
#     # --------------------------
#     def cancel_order(self, side: Side, price: float, volume: int):
#         book = self.bids if side == "B" else self.asks
#         price_list = self.bid_prices if side == "B" else self.ask_prices
#
#         if price not in book:
#             return
#
#         book[price] -= volume
#         if book[price] <= 0:
#             del book[price]
#             price_list.remove(price)
#
#     # --------------------------
#     # 成交减少 volume
#     # --------------------------
#     def trade(self, side: Side, price: float, volume: int):
#         """成交减少某个 order_id 对应的 volume，但价格不变"""
#         self.cancel_order(side, price, volume)
#
#     # --------------------------
#     # 获取快照（L1-LN）
#     # --------------------------
#     def get_snapshot(self, levels=None):
#         if levels is None:
#             levels = self.level
#         # 买盘从高到低
#         bids_px = self.bid_prices[-levels:][::-1]
#         asks_px = self.ask_prices[:levels]
#
#         bids_vol = [self.bids.get(p, 0) for p in bids_px]
#         asks_vol = [self.asks.get(p, 0) for p in asks_px]
#
#         return {
#             "bid_prices": bids_px,
#             "bid_volumes": bids_vol,
#             "ask_prices": asks_px,
#             "ask_volumes": asks_vol,
#         }
