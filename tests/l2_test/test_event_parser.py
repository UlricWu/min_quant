#!filepath: tests/l2_test/test_event_parser.py
import pandas as pd
from zoneinfo import ZoneInfo

from src.l2.common.event_parser import parse_events

SH_TZ = ZoneInfo("Asia/Shanghai")


# ============================================================
# Fixtures
# ============================================================
def sh_order_df():
    return pd.DataFrame({
        "TradeTime": ["2025-11-07 09:00:00.000"] * 2,
        "TickTime": ["091500040", "091501020"],  # 9:15:00.040 / 9:15:01.020
        "TickType": ["A", "D"],
        "Side": [1, 2],  # B / S
        "SubSeq": [1001, 1002],
        "Price": [10.2, 10.3],
        "Volume": [500, 200],
        "BuyNo": [0, 0],
        "SellNo": [0, 0],
        'SecurityID': [600001, 600002],
    })


def sh_trade_df():
    return pd.DataFrame({
        "TradeTime": ["2025-11-07 09:00:00.000"],
        "TickTime": ["091505040"],  # 9:15:05.040
        "TickType": ["T"],
        "Side": [1],
        "SubSeq": [8888],
        "Price": [10.25],
        "Volume": [300],
        "BuyNo": [30001],
        "SellNo": [45002],
        'SecurityID': [600001],
    })


def sz_order_df():
    return pd.DataFrame({
        "TradeTime": ["2025-11-07 09:00:00"],
        "OrderTime": ["093000500"],  # 09:30:00.500
        "OrderType": [1],
        "Side": [1],
        "SubSeq": [6001],
        "Price": [9.85],
        "Volume": [100],
        'SecurityID': [1],
    })


def sz_trade_df():
    return pd.DataFrame({
        "TradeTime": ["2025-11-07 09:00:00"],
        "TickTime": ["093001000"],  # 09:30:01.000
        "ExecType": [1],
        "Side": [None],
        "SubSeq": [7001],
        "TradePrice": [9.90],
        "TradeVolume": [200],
        "BuyNo": [111],
        "SellNo": [222],
        'SecurityID': [1],
    })


# ============================================================
# Tests
# ============================================================
def test_sh_order_parse():
    df = sh_order_df()

    out = parse_events(df, kind="order")

    assert len(out) == 2

    # Check time
    assert out["ts"].iloc[0].hour == 9
    assert out["ts"].iloc[0].minute == 15
    assert out["ts"].iloc[0].microsecond == 40

    # Check event
    assert out["event"].tolist() == ["ADD", "CANCEL"]


def test_sh_trade_parse():
    df = sh_trade_df()

    out = parse_events(df, kind="trade")

    assert len(out) == 1
    assert out["event"].iloc[0] == "TRADE"
    assert out["order_id"].iloc[0] == 8888
    assert out["buy_no"].iloc[0] == 30001


def test_sz_order_parse():
    df = sz_order_df()
    out = parse_events(df, kind="order")

    assert len(out) == 1
    assert out["event"].iloc[0] == "ADD"
    assert out["side"].iloc[0] == "B"


def test_sz_trade_parse():
    df = sz_trade_df()
    out = parse_events(df, kind="trade")

    assert len(out) == 1
    assert out["event"].iloc[0] == "TRADE"
    assert out["buy_no"].iloc[0] == 111
    assert out["sell_no"].iloc[0] == 222


def test_event_time_order():
    """事件排序正确：TickTime 决定 ts"""
    df = pd.DataFrame({
        "TradeTime": ["2025-11-07 09:00:00"] * 2,
        "TickTime": ["091500000", "091459999"],  # 后一条更早
        "TickType": ["A", "A"],
        "Side": [1, 1],
        "SubSeq": [1, 2],
        "Price": [10, 10],
        "Volume": [100, 100],
        "BuyNo": [0, 0],
        "SellNo": [0, 0],
        'SecurityID': [600001, 600002],
    })

    out = parse_events(df, "order").sort_values("ts")

    assert out["order_id"].tolist() == [2, 1]
