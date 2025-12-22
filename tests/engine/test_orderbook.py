#!filepath: tests/orderbook/test_orderbook.py

from logs.engines.orderbook_rebuild_engine import OrderBook
from src.l2.common.normalized_event import NormalizedEvent


# ======================================================
# Helpers
# ======================================================
def make_ev(
    *,
    event: str,
    order_id: int,
    side: str | None,
    price: float,
    volume: int,
    ts: int,
):
    return NormalizedEvent(
        ts=ts,
        event=event,
        order_id=order_id,
        side=side,
        price=price,
        volume=volume,
        buy_no=0,
        sell_no=0,
    )


# ======================================================
# ADD
# ======================================================
def test_add_order_basic():
    book = OrderBook(symbol="600001")

    ev = make_ev(
        event="ADD",
        order_id=1,
        side="B",
        price=10.0,
        volume=100,
        ts=1,
    )

    book.add_order(ev)

    assert 1 in book.orders
    assert book.orders[1].volume == 100
    assert book.bids[10.0] == [1]
    assert book.last_ts == 1


def test_add_duplicate_order_ignored():
    book = OrderBook("600001")

    ev = make_ev(event="ADD", order_id=1, side="B", price=10, volume=100, ts=1)
    book.add_order(ev)
    book.add_order(ev)

    assert len(book.orders) == 1
    assert book.bids[10] == [1]


def test_add_invalid_side_ignored():
    book = OrderBook("600001")

    ev = make_ev(event="ADD", order_id=1, side=None, price=10, volume=100, ts=1)
    book.add_order(ev)

    assert book.orders == {}
    assert book.bids == {}
    assert book.asks == {}


def test_add_fifo_same_price():
    book = OrderBook("600001")

    ev1 = make_ev(event="ADD", order_id=1, side="B", price=10, volume=100, ts=1)
    ev2 = make_ev(event="ADD", order_id=2, side="B", price=10, volume=200, ts=2)

    book.add_order(ev1)
    book.add_order(ev2)

    assert book.bids[10] == [1, 2]


# ======================================================
# CANCEL
# ======================================================
def test_cancel_order_basic():
    book = OrderBook("600001")

    book.add_order(make_ev(event="ADD", order_id=1, side="B", price=10, volume=100, ts=1))
    book.cancel_order(make_ev(event="CANCEL", order_id=1, side=None, price=0, volume=0, ts=2))

    assert book.orders == {}
    assert book.bids == {}
    assert book.last_ts == 2


def test_cancel_nonexistent_order_noop():
    book = OrderBook("600001")

    book.cancel_order(make_ev(event="CANCEL", order_id=999, side=None, price=0, volume=0, ts=1))

    assert book.orders == {}
    assert book.bids == {}
    assert book.last_ts is None


def test_cancel_removes_price_level():
    book = OrderBook("600001")

    book.add_order(make_ev(event="ADD", order_id=1, side="B", price=10, volume=100, ts=1))
    book.cancel_order(make_ev(event="CANCEL", order_id=1, side=None, price=0, volume=0, ts=2))

    assert 10 not in book.bids


# ======================================================
# TRADE
# ======================================================
def test_trade_partial_fill():
    book = OrderBook("600001")

    book.add_order(make_ev(event="ADD", order_id=1, side="B", price=10, volume=100, ts=1))
    book.trade(make_ev(event="TRADE", order_id=1, side=None, price=10, volume=40, ts=2))

    assert book.orders[1].volume == 60
    assert book.bids[10] == [1]
    assert book.last_ts == 2


def test_trade_full_fill_removes_order():
    book = OrderBook("600001")

    book.add_order(make_ev(event="ADD", order_id=1, side="B", price=10, volume=100, ts=1))
    book.trade(make_ev(event="TRADE", order_id=1, side=None, price=10, volume=100, ts=2))

    assert book.orders == {}
    assert book.bids == {}
    assert book.last_ts == 2


def test_trade_nonexistent_order_noop():
    book = OrderBook("600001")

    book.trade(make_ev(event="TRADE", order_id=999, side=None, price=10, volume=100, ts=1))

    assert book.orders == {}
    assert book.last_ts is None


# ======================================================
# SNAPSHOT
# ======================================================
def test_snapshot_basic():
    book = OrderBook("600001")

    book.add_order(make_ev(event="ADD", order_id=1, side="B", price=10, volume=100, ts=1))
    book.add_order(make_ev(event="ADD", order_id=2, side="B", price=9, volume=200, ts=2))
    book.add_order(make_ev(event="ADD", order_id=3, side="S", price=11, volume=150, ts=3))

    snap = book.to_snapshot()

    # 3 行：2 bid + 1 ask
    assert len(snap) == 3

    # bid 排序
    bids = snap[snap["side"] == "B"]
    assert bids.iloc[0]["price"] == 10
    assert bids.iloc[1]["price"] == 9

    # ask 排序
    asks = snap[snap["side"] == "S"]
    assert asks.iloc[0]["price"] == 11

    # ts
    assert snap["ts"].iloc[0] == book.last_ts


def test_snapshot_aggregates_volume_same_price():
    book = OrderBook("600001")

    book.add_order(make_ev(event="ADD", order_id=1, side="B", price=10, volume=100, ts=1))
    book.add_order(make_ev(event="ADD", order_id=2, side="B", price=10, volume=50, ts=2))

    snap = book.to_snapshot()

    bid = snap.iloc[0]
    assert bid["price"] == 10
    assert bid["volume"] == 150


def test_snapshot_depth_limit():
    book = OrderBook("600001")

    for i in range(20):
        book.add_order(
            make_ev(
                event="ADD",
                order_id=i,
                side="B",
                price=100 - i,
                volume=10,
                ts=i,
            )
        )

    snap = book.to_snapshot(depth=5)
    bids = snap[snap["side"] == "B"]

    assert len(bids) == 5
    assert bids.iloc[0]["price"] == 100
    assert bids.iloc[-1]["price"] == 96
