import pandas as pd
import pytest
from dataclasses import dataclass
from pathlib import Path

from src.engines.trade_enrich_engine import TradeEnrichEngine, TradeEnrichConfig
from src.engines.context import EngineContext
from src.l2.common.normalized_event import NormalizedEvent


# =========================================================
# Helpers
# =========================================================

def make_trade_event(
    ts: int,
    price: float,
    volume: int,
    side: str | None = None,
):
    """
    构造一个最小可用的 TRADE NormalizedEvent
    ts: ns
    """
    return NormalizedEvent(
        ts=ts,
        event="TRADE",
        order_id=1,
        side=side,
        price=price,
        volume=volume,
        buy_no=0,
        sell_no=0,
    )


def make_non_trade_event(ts: int):
    return NormalizedEvent(
        ts=ts,
        event="ADD",
        order_id=1,
        side="B",
        price=10.0,
        volume=100,
        buy_no=0,
        sell_no=0,
    )


# =========================================================
# Realtime tests
# =========================================================

def test_realtime_ignore_non_trade():
    """
    非 TRADE 事件必须被完全忽略
    """
    engine = TradeEnrichEngine()

    ctx = EngineContext(
        symbol='000000',
        mode="realtime",
        event=make_non_trade_event(1_000_000_000),
    )

    engine.execute(ctx)
    assert engine._rows == []


def test_realtime_signed_volume():
    """
    signed_volume 逻辑：
    B -> +volume
    S -> -volume
    None -> 0
    """
    engine = TradeEnrichEngine()

    events = [
        make_trade_event(1_000_000_000, 10.0, 100, "B"),
        make_trade_event(1_000_001_000, 10.0, 200, "S"),
        make_trade_event(1_000_002_000, 10.0, 300, None),
    ]

    for ev in events:

        engine.execute(EngineContext(mode="realtime", event=ev, symbol='000000'))

    df = pd.DataFrame(engine._rows)

    assert df["signed_volume"].tolist() == [100, -200, 0]


def test_realtime_burst_id_increment():
    """
    burst_id：
    - 间隔 <= window_ms → 同一簇
    - 间隔 > window_ms → 新簇
    """
    cfg = TradeEnrichConfig(burst_window_ms=10)
    engine = TradeEnrichEngine(cfg)

    base = 1_000_000_000_000_000  # ns

    events = [
        make_trade_event(base + 0 * 1_000_000, 10.0, 100),
        make_trade_event(base + 5 * 1_000_000, 10.0, 100),   # same burst
        make_trade_event(base + 20 * 1_000_000, 10.0, 100), # new burst
    ]

    for ev in events:
        engine.execute(EngineContext(mode="realtime", event=ev,symbol='000000'))

    df = pd.DataFrame(engine._rows)
    assert df["burst_id"].tolist() == [0, 0, 1]


def test_realtime_trade_bucket_unknown():
    """
    realtime 模式下未初始化分位数：
    trade_bucket 必须是 'U'
    """
    engine = TradeEnrichEngine()

    ev = make_trade_event(1_000_000_000, 10.0, 100)
    engine.execute(EngineContext(mode="realtime", event=ev,symbol='000000'))

    row = engine._rows[0]
    assert row["trade_bucket"] == "U"


# =========================================================
# Offline tests
# =========================================================

def test_offline_basic(tmp_path: Path):
    """
    offline 基本流程：
    - 只处理 TRADE
    - 正确生成 parquet
    """
    input_path = tmp_path / "events.parquet"
    output_path = tmp_path / "trade.parquet"

    df = pd.DataFrame(
        [
            # 非 trade
            dict(ts=1, event="ADD", order_id=1, side="B", price=10.0, volume=100, buy_no=0, sell_no=0),
            # trade
            dict(ts=2, event="TRADE", order_id=2, side="B", price=10.0, volume=200, buy_no=0, sell_no=0),
        ]
    )
    df.to_parquet(input_path, index=False)

    engine = TradeEnrichEngine()

    ctx = EngineContext(
        mode="offline",
        symbol="TEST",
        date="2025-01-01",
        input_path=input_path,
        output_path=output_path,
    )

    engine.execute(ctx)

    out = pd.read_parquet(output_path)
    assert len(out) == 1
    assert out.iloc[0]["volume"] == 200
    assert out.iloc[0]["notional"] == 2000.0


def test_offline_trade_bucket_quantiles(tmp_path: Path):
    """
    offline 模式：
    trade_bucket 基于全体成交量分位数
    """
    input_path = tmp_path / "events.parquet"
    output_path = tmp_path / "trade.parquet"

    df = pd.DataFrame(
        [
            dict(ts=1, event="TRADE", order_id=1, side="B", price=10, volume=10, buy_no=0, sell_no=0),
            dict(ts=2, event="TRADE", order_id=2, side="B", price=10, volume=50, buy_no=0, sell_no=0),
            dict(ts=3, event="TRADE", order_id=3, side="B", price=10, volume=100, buy_no=0, sell_no=0),
        ]
    )
    df.to_parquet(input_path, index=False)

    cfg = TradeEnrichConfig(
        medium_trade_pct=0.5,
        large_trade_pct=0.9,
    )
    engine = TradeEnrichEngine(cfg)

    ctx = EngineContext(
        mode="offline",
        symbol="TEST",
        date="2025-01-01",
        input_path=input_path,
        output_path=output_path,
    )

    engine.execute(ctx)

    out = pd.read_parquet(output_path)

    assert out["trade_bucket"].tolist() == ["S", "M", "L"]


def test_offline_empty_trade(tmp_path: Path):
    """
    没有 TRADE 时：
    - 不报错
    - 输出空 parquet
    """
    input_path = tmp_path / "events.parquet"
    output_path = tmp_path / "trade.parquet"

    df = pd.DataFrame(
        [
            dict(ts=1, event="ADD", order_id=1, side="B", price=10, volume=10, buy_no=0, sell_no=0),
        ]
    )
    df.to_parquet(input_path, index=False)

    engine = TradeEnrichEngine()

    ctx = EngineContext(
        mode="offline",
        symbol="TEST",
        date="2025-01-01",
        input_path=input_path,
        output_path=output_path,
    )

    engine.execute(ctx)

    out = pd.read_parquet(output_path)
    assert out.empty
