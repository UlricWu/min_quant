#!filepath: tests/l2_test/test_trade_enricher.py
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

from src.l2.orderbook.trade_enricher import TradeEnricher


TZ = ZoneInfo("Asia/Shanghai")


def make_trade_df():
    return pd.DataFrame({
        "ts": [
            datetime(2025, 11, 7, 9, 30, 0, 1000, tzinfo=TZ),
            datetime(2025, 11, 7, 9, 30, 0, 2000, tzinfo=TZ),
            datetime(2025, 11, 7, 9, 30, 0, 5000, tzinfo=TZ),
        ],
        "TradePrice": [10.05, 9.95, 10.20],
        "TradeVolume": [100, 2000, 8000],
        "BidPrice1": [10.00, 10.00, 10.10],
        "AskPrice1": [10.10, 10.10, 10.30],
        "PreTradePrice": [10.00, 10.05, 10.00],
    })


def test_aggressor_side():
    df = make_trade_df()
    enricher = TradeEnricher()
    out = enricher.enrich(df)

    assert out["aggressor"].tolist() == ["B", "S", "B"]


def test_impact_flag():
    df = make_trade_df()
    enricher = TradeEnricher()
    out = enricher.enrich(df)

    assert out["is_price_impact"].tolist() == [False, True, False]


def test_impact_value():
    df = make_trade_df()
    enricher = TradeEnricher()
    out = enricher.enrich(df)

    # impact = (price - mid) / mid
    mid0 = (10.00 + 10.10) / 2
    assert abs(out["impact"].iloc[0] - ((10.05 - mid0) / mid0)) < 1e-6


def test_trade_bucket():
    df = make_trade_df()
    enricher = TradeEnricher()
    out = enricher.enrich(df)

    # 100 → S, 2000 → M, 8000 → L
    assert set(out["trade_bucket"]) == {"S", "M", "L"}


def test_burst_id():
    df = make_trade_df()
    enricher = TradeEnricher(burst_window_ms=2)
    out = enricher.enrich(df)

    assert out["burst_id"].tolist() == [0, 0, 1]


def test_vpin():
    df = make_trade_df()
    enricher = TradeEnricher(vpin_bucket_volume=1000)
    out = enricher.enrich(df)

    # 三个成交的总量是 100 + 2000 + 8000
    # 第一笔成交 100 → 不满 bucket → NaN
    # 第二笔累计 2100 → >= 1000 → 触发 vpin
    # 第三笔开始新桶

    assert out["vpin"].notna().sum() >= 1
