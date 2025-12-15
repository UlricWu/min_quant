from datetime import datetime, timedelta

from src.engines.orderbook_engine import (
    OrderBookRebuildEngine,
    InternalEvent,
)


def ts(sec: int) -> datetime:
    """简单时间工具"""
    base = datetime(2025, 1, 1, 9, 30, 0)
    return base + timedelta(seconds=sec)


# ============================================================================
# 1. ADD 行为
# ============================================================================
def test_orderbook_add_bid_and_ask():
    engine = OrderBookRebuildEngine()

    events = [
        InternalEvent(
            ts=ts(1),
            event="ADD",
            order_id=1,
            side="B",
            price=10.0,
            volume=100,
            buy_no=0,
            sell_no=0,
        ),
        InternalEvent(
            ts=ts(2),
            event="ADD",
            order_id=2,
            side="S",
            price=10.5,
            volume=200,
            buy_no=0,
            sell_no=0,
        ),
    ]

    snaps = list(engine.rebuild(events))

    snap = snaps[-1]
    assert snap.bids == [(10.0, 100)]
    assert snap.asks == [(10.5, 200)]


# ============================================================================
# 2. CANCEL 行为
# ============================================================================
def test_orderbook_cancel_reduces_volume():
    engine = OrderBookRebuildEngine()

    events = [
        InternalEvent(ts(1), "ADD", 1, "B", 10.0, 100, 0, 0),
        InternalEvent(ts(2), "CANCEL", 1, "B", 10.0, 40, 0, 0),
    ]

    snaps = list(engine.rebuild(events))
    snap = snaps[-1]

    assert snap.bids == [(10.0, 60)]
    assert snap.asks == []


def test_orderbook_cancel_removes_level():
    engine = OrderBookRebuildEngine()

    events = [
        InternalEvent(ts(1), "ADD", 1, "S", 10.5, 50, 0, 0),
        InternalEvent(ts(2), "CANCEL", 1, "S", 10.5, 50, 0, 0),
    ]

    snaps = list(engine.rebuild(events))
    snap = snaps[-1]

    assert snap.asks == []
    assert snap.bids == []


# ============================================================================
# 3. TRADE 行为（从对手盘扣减）
# ============================================================================
def test_trade_hits_ask_when_buy():
    """
    买方成交（side='B'） → 吃掉 ask
    """
    engine = OrderBookRebuildEngine()

    events = [
        InternalEvent(ts(1), "ADD", 1, "S", 10.5, 200, 0, 0),
        InternalEvent(ts(2), "TRADE", 99, "B", 10.5, 80, 0, 0),
    ]

    snaps = list(engine.rebuild(events))
    snap = snaps[-1]

    assert snap.asks == [(10.5, 120)]
    assert snap.bids == []


def test_trade_hits_bid_when_sell():
    """
    卖方成交（side='S'） → 吃掉 bid
    """
    engine = OrderBookRebuildEngine()

    events = [
        InternalEvent(ts(1), "ADD", 1, "B", 10.0, 150, 0, 0),
        InternalEvent(ts(2), "TRADE", 99, "S", 10.0, 70, 0, 0),
    ]

    snaps = list(engine.rebuild(events))
    snap = snaps[-1]

    assert snap.bids == [(10.0, 80)]
    assert snap.asks == []


# ============================================================================
# 4. 快照顺序 & 排序（隐含正确性）
# ============================================================================
def test_bid_ask_sorted_correctly():
    engine = OrderBookRebuildEngine()

    events = [
        InternalEvent(ts(1), "ADD", 1, "B", 10.0, 100, 0, 0),
        InternalEvent(ts(2), "ADD", 2, "B", 10.2, 50, 0, 0),
        InternalEvent(ts(3), "ADD", 3, "S", 10.5, 80, 0, 0),
        InternalEvent(ts(4), "ADD", 4, "S", 10.3, 60, 0, 0),
    ]

    snap = list(engine.rebuild(events))[-1]

    assert snap.bids == [(10.2, 50), (10.0, 100)]
    assert snap.asks == [(10.3, 60), (10.5, 80)]
