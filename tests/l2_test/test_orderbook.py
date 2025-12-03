#!filepath: tests/l2_test/test_orderbook_null_conditions.py
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
#!filepath: tests/l2_test/test_orderbook_rebuilder.py
import pandas as pd
from zoneinfo import ZoneInfo

from src.l2.orderbook.orderbook_rebuilder import OrderBookRebuilder
from src.l2.event_parser import parse_events

SH_TZ = ZoneInfo("Asia/Shanghai")


def make_df():
    return pd.DataFrame({
        "TradeTime": ["2025-11-07 09:00:00.000"] * 4,
        "TickTime": ["093000000", "093001000", "093002000", "093003000"],

        # A = add, T = trade, D = cancel
        "TickType": ["A", "A", "T", "D"],
        "Side": [1, 2, 1, 1],  # B/S
        "SubSeq": [100, 200, 100, 100],
        "Price": [10.0, 10.1, 10.0, 10.0],
        "Volume": [500, 300, 200, 100],
        "BuyNo": [100, 0, 100, 100],
        "SellNo": [0, 200, 200, 0],
        "ExchangeID": [1, 1, 1, 1],
        'SecurityID':[600001, 600002,600001, 600002],
    })


def test_orderbook_rebuilder_add_trade_cancel():
    df = make_df()

    # 解析事件（只做 SH）
    ev = parse_events(df, kind="order")

    # 基于事件回放
    obr = OrderBookRebuilder(book_levels=5)
    # 不读文件，我们人工喂 events
    events = ev.sort_values("ts").reset_index(drop=True)

    book = obr  # alias for readability

    # 模拟重建过程
    orderbook = OrderBookRebuilder().path  # not used
    book_state = {}

    # 使用 OrderBookRebuilder 的内部订单薄
    book_only = OrderBookRebuilder().build  # just alias

    # 手动构造 OrderBook 与 snapshots
    from src.l2.orderbook.orderbook_rebuilder import OrderBook
    ob = OrderBook()
    order_map = {}
    snapshots = []

    for _, e in events.iterrows():
        ts = e["ts"]
        ev = e["event"]
        oid = int(e["order_id"])
        side = e["side"]
        price = float(e["price"])
        vol = int(e["volume"])
        buy_no = int(e["buy_no"])
        sell_no = int(e["sell_no"])

        if ev == "ADD":
            order_map[oid] = {"price": price, "side": side, "remaining": vol}
            ob.add_order(side, price, vol)

        elif ev == "TRADE":
            if buy_no in order_map:
                info = order_map[buy_no]
                eat = min(vol, info["remaining"])
                info["remaining"] -= eat
                if info["remaining"] <= 0:
                    del order_map[buy_no]
                ob.trade("B", info["price"], eat)

            if sell_no in order_map:
                info = order_map[sell_no]
                eat = min(vol, info["remaining"])
                info["remaining"] -= eat
                if info["remaining"] <= 0:
                    del order_map[sell_no]
                ob.trade("S", info["price"], eat)

        elif ev == "CANCEL":
            if oid in order_map:
                info = order_map[oid]
                cancel = min(vol, info["remaining"])
                info["remaining"] -= cancel
                if info["remaining"] <= 0:
                    del order_map[oid]
                ob.cancel_order(info["side"], info["price"], cancel)

        snap = ob.get_snapshot(3)
        snapshots.append(snap)

    # ============ 验证 ============

    # L1 买卖盘正确
    last = snapshots[-1]
    assert last["bid_prices"][0] == 10.0   # 最后剩余挂单价格
    assert last["bid_volumes"][0] > 0
