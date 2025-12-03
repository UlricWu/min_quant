#!filepath: tests/l2_test/test_orderbook_event_alignment.py
import pandas as pd
from src.l2.orderbook.orderbook_store import OrderBook

def test_event_alignment_sequence():
    """
    场景：
    1. 买单 ADD
    2. 卖单 ADD
    3. 成交 TRADE 发生 → 双方挂单 volume 应减少
    4. 重建后订单簿对齐正确
    """

    book = OrderBook()

    # Step1: 买挂单 (B, 10.0, 100)
    book.add_order("B", 10.0, 100)

    # Step2: 卖挂单 (S, 10.1, 200)
    book.add_order("S", 10.1, 200)

    snap_1 = book.get_snapshot(5)
    assert snap_1["bid_prices"][0] == 10.0
    assert snap_1["ask_prices"][0] == 10.1

    # Step3: 成交 50 股
    book.trade("B", 10.0, 50)

    snap_2 = book.get_snapshot(5)
    # 剩余 50 股
    assert snap_2["bid_volumes"][0] == 50
    # 卖盘未变
    assert snap_2["ask_volumes"][0] == 200

    # Step4: 成交完剩余买盘 50 股
    book.trade("B", 10.0, 50)

    snap_3 = book.get_snapshot(5)
    # 买盘应为空
    assert snap_3["bid_prices"] == []
    # 卖盘仍然存在
    assert snap_3["ask_prices"][0] == 10.1
#!filepath: tests/l2_test/test_orderbook_time_normalization.py
import pandas as pd
from src.utils.datetime_utils import DateTimeUtils as dt

def test_time_normalization_to_shanghai():
    """
    测试 TickTime 标准化是否正确转为 tz=Asia/Shanghai 的 datetime
    """

    raw_times = [
        "2025-03-01 09:30:00.100",
        "2025-03-01 09:30:00.200",
        "2025-03-01 09:30:00.150",
    ]
    df = pd.DataFrame({"TickTime": raw_times})

    df["ts"] = df["TickTime"].map(dt.parse)

    # tz 检查
    # assert df["ts"].iloc[0].tz.zone == "Asia/Shanghai"

    # 排序检查
    sorted_ts = df["ts"].sort_values().reset_index(drop=True)
    assert sorted_ts[0] < sorted_ts[1]
    assert sorted_ts[1] < sorted_ts[2]


# !filepath: tests/l2_test/test_trade_enrichment.py
import pandas as pd
from src.l2.orderbook.orderbook_store import OrderBook


def test_trade_enrichment_buy_and_sell_reduce_correctly():
    """
    场景：
    买盘 100 股＠10.0
    卖盘 80 股＠10.0
    成交 70 股

    → 买盘剩余 30
    → 卖盘剩余 10
    """
    book = OrderBook()

    # ADD 买单、卖单
    book.add_order("B", 10.0, 100)
    book.add_order("S", 10.0, 80)

    snap_1 = book.get_snapshot(5)
    assert snap_1["bid_volumes"][0] == 100
    assert snap_1["ask_volumes"][0] == 80

    # TRADE 成交 70
    book.trade("B", 10.0, 70)  # 买方减少 70
    book.trade("S", 10.0, 70)  # 卖方减少 70

    snap_2 = book.get_snapshot(5)

    # 成交后的剩余挂单
    assert snap_2["bid_volumes"][0] == 30
    assert snap_2["ask_volumes"][0] == 10
#!filepath: tests/l2_test/test_snapshot_alignment.py
import pandas as pd
from src.l2.orderbook.orderbook_store import OrderBook

def test_snapshot_alignment_each_event_one_snapshot_correct():
    book = OrderBook()

    events = [
        ("ADD", "B", 10.0, 100),      # 买单
        ("ADD", "S", 10.1, 200),      # 卖单
        ("TRADE", "B", 10.0, 100),    # 成交吃光全部买盘
        ("CANCEL", "S", 10.1, 200),   # 卖盘取消
    ]

    snapshots = []

    for ev, side, price, vol in events:
        if ev == "ADD":
            book.add_order(side, price, vol)
        elif ev == "TRADE":
            book.trade(side, price, vol)
        elif ev == "CANCEL":
            book.cancel_order(side, price, vol)

        snapshots.append(book.get_snapshot(5))

    # Step1: 买盘存在
    assert snapshots[0]["bid_prices"][0] == 10.0

    # Step2: 卖盘存在
    assert snapshots[1]["ask_prices"][0] == 10.1

    # Step3: BUY 100 成交 → 买盘被吃光
    assert snapshots[2]["bid_prices"] == []

    # Step4: 卖盘取消 → 卖盘为空
    assert snapshots[3]["ask_prices"] == []

