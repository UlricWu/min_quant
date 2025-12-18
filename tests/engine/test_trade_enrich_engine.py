#!filepath: tests/engines/test_trade_enrich_engine.py
import pandas as pd
from pathlib import Path

from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.engines.context import EngineContext
from src.l2.common.normalized_event import NormalizedEvent


# =========================================================
# Fixtures
# =========================================================
def make_event(event, side="B", price=10.0, volume=100, ts=1):
    return NormalizedEvent(
        ts=ts,
        event=event,
        order_id=1,
        side=side,
        price=price,
        volume=volume,
        buy_no=0,
        sell_no=0,
    )


# =========================================================
# Tests
# =========================================================
def test_trade_enrich_only_trade(tmp_path: Path):
    events = [
        make_event("ADD"),
        make_event("TRADE", side="B", price=10, volume=100),
        make_event("CANCEL"),
        make_event("TRADE", side="S", price=20, volume=50),
    ]

    df = pd.DataFrame([e.to_dict() for e in events])
    in_path = tmp_path / "Events.parquet"
    out_path = tmp_path / "Enriched.parquet"
    df.to_parquet(in_path)

    ctx = EngineContext(
        mode="offline",
        symbol="600001",
        date="2025-11-07",
        input_path=in_path,
        output_path=out_path,
    )

    engine = TradeEnrichEngine()
    engine.execute(ctx)

    out_df = pd.read_parquet(out_path)

    assert len(out_df) == 2  # 只处理 TRADE
    assert set(out_df.columns) == {
        "ts_ns",
        "price",
        "volume",
        "side",
        "notional",
        "signed_volume",
    }


def test_trade_enrich_values():
    ev = make_event("TRADE", side="B", price=10, volume=5)

    engine = TradeEnrichEngine()
    ctx = EngineContext(
        mode="realtime",
        symbol="600001",
        date="2025-11-07",
        event=ev,
    )

    engine.execute(ctx)

    row = engine._rows[0]

    assert row["notional"] == 50
    assert row["signed_volume"] == 5


def test_trade_enrich_sell_signed_volume():
    ev = make_event("TRADE", side="S", price=10, volume=5)

    engine = TradeEnrichEngine()
    engine.execute(
        EngineContext(
            mode="realtime",
            symbol="600001",
            date="2025-11-07",
            event=ev,
        )
    )

    row = engine._rows[0]
    assert row["signed_volume"] == -5
