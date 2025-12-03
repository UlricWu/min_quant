#!filepath: tests/l2_test/test_orderbook_null_conditions.py
import pandas as pd
from src.l2.orderbook.orderbook_store import OrderBook


def test_empty_book_initial_state():
    """
    测试场景 1：
    开盘后的第一条事件（或过滤集合竞价后无事件重放）
    → 订单簿为空 → BidPrice1/AskPrice1 都应为 None
    """
    book = OrderBook()
    snap = book.get_snapshot(5)

    assert snap["bid_prices"] == []
    assert snap["ask_prices"] == []
    assert snap["bid_volumes"] == []
    assert snap["ask_volumes"] == []

    # L1 应为 None
    assert snap["bid_prices"][0] if snap["bid_prices"] else None is None
    assert snap["ask_prices"][0] if snap["ask_prices"] else None is None


def test_only_sell_orders():
    """
    测试场景 2：
    市场只有卖盘 → AskPrice1 应正常，BidPrice1 应为 None
    """
    book = OrderBook()

    # 卖盘挂 10.0 价格
    book.add_order("S", 10.0, 100)

    snap = book.get_snapshot(5)

    # 卖盘正常
    assert snap["ask_prices"][0] == 10.0
    assert snap["ask_volumes"][0] == 100

    # 买盘为空
    assert snap["bid_prices"] == []
    assert snap["bid_volumes"] == []


def test_buy_side_depleted_by_trade():
    """
    测试场景 3：
    有买盘，但随后被主动卖盘全部吃光
    → BidPrice1 应变为 NULL
    """
    book = OrderBook()

    # 买盘挂 9.5
    book.add_order("B", 9.5, 200)

    # 卖盘以 9.5 成交，吃光买盘
    book.trade("B", 9.5, 200)   # 买盘被减少为 0 → 删除价格档位

    snap = book.get_snapshot(5)

    # 买盘应被清空
    assert snap["bid_prices"] == []
    assert snap["bid_volumes"] == []

    # 卖盘为空（无挂单）
    assert snap["ask_prices"] == []
    assert snap["ask_volumes"] == []


def test_partial_trade_leaves_remaining_buy():
    """
    测试场景 4：
    部分成交 → 买盘仍然存在 → 不应出现 NULL
    """
    book = OrderBook()

    book.add_order("B", 9.5, 200)
    book.trade("B", 9.5, 50)  # 部分成交

    snap = book.get_snapshot(5)

    assert snap["bid_prices"][0] == 9.5
    assert snap["bid_volumes"][0] == 150


def test_sell_side_absent_initially_and_after_trade():
    """
    测试场景 5：
    买盘存在，但卖盘一直为空
    → AskPrice1 = NULL 合理
    """
    book = OrderBook()

    book.add_order("B", 9.5, 100)

    snap = book.get_snapshot(5)

    assert snap["ask_prices"] == []
    assert snap["ask_volumes"] == []

    # Bid 正常
    assert snap["bid_prices"][0] == 9.5
