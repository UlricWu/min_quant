"""

我们将锁死以下 7 条不变量：

ADD → 只增加对应方向盘口

CANCEL → 必须完全移除该 order_id 的影响

TRADE → 只作用于被成交 order_id 所在方向

TRADE 部分成交 → 聚合量递减，但订单仍存在

TRADE 完全成交 → 订单彻底消失

TRADE 超额 → 不得出现负聚合量

同价位多订单 → FIFO 不变
"""
import pytest
from src.engines.orderbook_rebuild_engine import OrderBook


@pytest.mark.contract
def test_add_only_affects_own_side():
    book = OrderBook()

    book.add_order(ts=1, order_id=1, side="B", price=10.0, volume=100)
    book.add_order(ts=2, order_id=2, side="S", price=10.2, volume=80)

    assert book.bid_qty == {10.0: 100}
    assert book.ask_qty == {10.2: 80}
@pytest.mark.contract
def test_cancel_removes_order_completely():
    book = OrderBook()

    book.add_order(ts=1, order_id=1, side="B", price=10.0, volume=100)
    book.cancel_order(ts=2, order_id=1)

    assert book.bid_qty == {}
    assert book.orders == {}
@pytest.mark.contract
def test_trade_affects_only_passive_order_side():
    book = OrderBook()

    # 卖单
    book.add_order(ts=1, order_id=1, side="S", price=10.2, volume=100)
    # 买单
    book.add_order(ts=2, order_id=2, side="B", price=10.0, volume=100)

    # 成交发生在买单上（被动）
    book.trade(ts=3, order_id=2, volume=30)

    assert book.bid_qty[10.0] == 70
    assert book.ask_qty[10.2] == 100
@pytest.mark.contract
def test_trade_partial_fill_keeps_order():
    book = OrderBook()

    book.add_order(ts=1, order_id=1, side="B", price=10.0, volume=100)
    book.trade(ts=2, order_id=1, volume=40)

    assert book.bid_qty[10.0] == 60
    assert 1 in book.orders
@pytest.mark.contract
def test_trade_full_fill_removes_order():
    book = OrderBook()

    book.add_order(ts=1, order_id=1, side="S", price=10.2, volume=80)
    book.trade(ts=2, order_id=1, volume=80)

    assert book.ask_qty == {}
    assert book.orders == {}
@pytest.mark.contract
def test_trade_overfill_never_negative():
    book = OrderBook()

    book.add_order(ts=1, order_id=1, side="B", price=10.0, volume=50)
    book.trade(ts=2, order_id=1, volume=100)  # 超额

    assert book.bid_qty == {}
    assert book.orders == {}
@pytest.mark.contract
def test_fifo_order_preserved_on_same_price():
    book = OrderBook()

    book.add_order(ts=1, order_id=1, side="B", price=10.0, volume=50)
    book.add_order(ts=2, order_id=2, side="B", price=10.0, volume=50)

    # 先成交第一个
    book.trade(ts=3, order_id=1, volume=50)

    assert book.bid_qty[10.0] == 50
    assert 1 not in book.orders
    assert 2 in book.orders
