#!filepath: tests/engines/test_orderbook_rebuild_engine.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from logs.engines import EngineContext
from logs.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.l2.common.normalized_event import NormalizedEvent

# ======================================================
# Minimal EngineContext (if your project already has it, import it instead)
# ======================================================
@dataclass
class _Ctx:
    mode: str
    symbol: str
    date: str | None = None
    input_path: Path | None = None
    output_path: Path | None = None
    event: Any | None = None
    emit_snapshot: bool = False


def make_ev(
    *,
    ts: int,
    event: str,
    order_id: int,
    side: str | None,
    price: float,
    volume: int,
    buy_no: int = 0,
    sell_no: int = 0,
) -> NormalizedEvent:
    return NormalizedEvent(
        ts=ts,
        event=event,
        order_id=order_id,
        side=side,
        price=price,
        volume=volume,
        buy_no=buy_no,
        sell_no=sell_no,
    )


# ======================================================
# Fixtures
# ======================================================
@pytest.fixture()
def engine() -> OrderBookRebuildEngine:
    return OrderBookRebuildEngine()


@pytest.fixture()
def symbol() -> str:
    return "603322"


@pytest.fixture()
def events_df() -> pd.DataFrame:
    # Deliberately unordered by ts to ensure engine iter order is file order,
    # but snapshot ts should end up being the last applied event ts.
    evs = [
        make_ev(ts=2, event="ADD", order_id=1, side="B", price=10.0, volume=100),
        make_ev(ts=1, event="ADD", order_id=2, side="S", price=10.2, volume=50),
        make_ev(ts=3, event="TRADE", order_id=1, side=None, price=10.0, volume=40),
        make_ev(ts=4, event="CANCEL", order_id=2, side=None, price=0.0, volume=0),
    ]
    return pd.DataFrame([e.to_dict() for e in evs])


# ======================================================
# Tests: Realtime mode
# ======================================================
def test_execute_realtime_initializes_book(engine: OrderBookRebuildEngine, symbol: str):
    ctx = _Ctx(mode="realtime", symbol=symbol, event=make_ev(ts=1, event="ADD", order_id=1, side="B", price=10, volume=100))
    engine.execute(ctx)

    assert engine.book is not None
    assert engine.book.symbol == symbol
    assert 1 in engine.book.orders


def test_execute_realtime_no_snapshot_when_emit_false(tmp_path: Path, engine: OrderBookRebuildEngine, symbol: str):
    out = tmp_path / "Snapshot.parquet"
    ctx = _Ctx(
        mode="realtime",
        symbol=symbol,
        event=make_ev(ts=1, event="ADD", order_id=1, side="B", price=10, volume=100),
        output_path=out,
        emit_snapshot=False,
    )
    engine.execute(ctx)

    assert not out.exists()


def test_execute_realtime_emits_snapshot_when_emit_true(tmp_path: Path, engine: OrderBookRebuildEngine, symbol: str):
    out = tmp_path / "Snapshot.parquet"
    ctx = _Ctx(
        mode="realtime",
        symbol=symbol,
        event=make_ev(ts=1, event="ADD", order_id=1, side="B", price=10, volume=100),
        output_path=out,
        emit_snapshot=True,
    )
    engine.execute(ctx)

    assert out.exists()
    df = pd.read_parquet(out)
    assert set(df.columns) >= {"symbol", "ts", "side", "level", "price", "volume"}
    assert df["symbol"].unique().tolist() == [symbol]


def test_execute_realtime_unknown_event_raises(engine: OrderBookRebuildEngine, symbol: str):
    ctx = _Ctx(
        mode="realtime",
        symbol=symbol,
        event=make_ev(ts=1, event="BAD", order_id=1, side="B", price=10, volume=100),
    )
    with pytest.raises(ValueError, match=r"Unknown event="):
        engine.execute(ctx)


def test_emit_snapshot_out_none_is_noop(engine: OrderBookRebuildEngine, symbol: str):
    # Ensure it doesn't crash if emit_snapshot True but output_path is None
    ctx = _Ctx(
        mode="realtime",
        symbol=symbol,
        event=make_ev(ts=1, event="ADD", order_id=1, side="B", price=10, volume=100),
        output_path=None,
        emit_snapshot=True,
    )
    engine.execute(ctx)
    assert engine.book is not None
    assert engine.book.last_ts == 1

def test_orderbook_realtime_rebuild():
    engine = OrderBookRebuildEngine()

    events = [
        make_ev(ts=1, event="ADD", order_id=1, side="B", price=10.0, volume=100),
        make_ev(ts=2, event="TRADE", order_id=1, side=None, price=10.0, volume=40),
        make_ev(ts=3, event="ADD", order_id=2, side="S", price=10.2, volume=50),
        make_ev(ts=4, event="CANCEL", order_id=2, side=None, price=0.0, volume=0),
    ]

    ctx = EngineContext(
        mode="realtime",
        symbol="603322",
        emit_snapshot=False,
    )

    for ev in events:
        ctx.event = ev
        engine.execute(ctx)

    snap = engine.book.to_snapshot()

    bids = snap[snap["side"] == "B"]
    asks = snap[snap["side"] == "S"]

    assert len(bids) == 1
    assert bids.iloc[0]["price"] == 10.0
    assert bids.iloc[0]["volume"] == 60
    assert len(asks) == 0


# ======================================================
# Tests: Offline mode
# ======================================================
def test_execute_offline_reads_events_and_writes_snapshot(
    tmp_path: Path,
    engine: OrderBookRebuildEngine,
    symbol: str,
    events_df: pd.DataFrame,
):
    in_path = tmp_path / "Events.parquet"
    out_path = tmp_path / "Snapshot.parquet"
    events_df.to_parquet(in_path, index=False)

    ctx = _Ctx(
        mode="offline",
        symbol=symbol,
        date="2025-11-04",
        input_path=in_path,
        output_path=out_path,
        emit_snapshot=True,  # ignored in offline path; engine always emits at end
    )
    engine.execute(ctx)

    assert out_path.exists()

    snap = pd.read_parquet(out_path)
    assert set(snap.columns) >= {"symbol", "ts", "side", "level", "price", "volume"}
    assert snap["symbol"].unique().tolist() == [symbol]

    # last_ts should be the last applied event's ts in file order:
    # our events_df was [ts=2,1,3,4] so final last_ts should be 4
    assert snap["ts"].dropna().iloc[0] == 4


def test_execute_offline_applies_add_trade_cancel_effects(
    tmp_path: Path,
    engine: OrderBookRebuildEngine,
    symbol: str,
):
    # Scenario:
    # 1) ADD bid id=1 @10 vol=100
    # 2) TRADE id=1 vol=40 => remaining 60
    # 3) ADD ask id=2 @10.2 vol=50
    # 4) CANCEL ask id=2 => removed
    df = pd.DataFrame(
        [
            make_ev(ts=1, event="ADD", order_id=1, side="B", price=10.0, volume=100).to_dict(),
            make_ev(ts=2, event="TRADE", order_id=1, side=None, price=10.0, volume=40).to_dict(),
            make_ev(ts=3, event="ADD", order_id=2, side="S", price=10.2, volume=50).to_dict(),
            make_ev(ts=4, event="CANCEL", order_id=2, side=None, price=0.0, volume=0).to_dict(),
        ]
    )

    in_path = tmp_path / "Events.parquet"
    out_path = tmp_path / "Snapshot.parquet"
    df.to_parquet(in_path, index=False)

    ctx = _Ctx(mode="offline", symbol=symbol, date="2025-11-04", input_path=in_path, output_path=out_path)
    engine.execute(ctx)

    snap = pd.read_parquet(out_path)

    bids = snap[snap["side"] == "B"].sort_values("level")
    asks = snap[snap["side"] == "S"].sort_values("level")

    # Bid remains with remaining volume 60
    assert len(bids) == 1
    assert bids.iloc[0]["price"] == 10.0
    assert bids.iloc[0]["volume"] == 60

    # Ask canceled -> none
    assert len(asks) == 0


def test_execute_offline_unknown_event_raises(
    tmp_path: Path,
    engine: OrderBookRebuildEngine,
    symbol: str,
):
    df = pd.DataFrame(
        [
            make_ev(ts=1, event="ADD", order_id=1, side="B", price=10.0, volume=100).to_dict(),
            make_ev(ts=2, event="WTF", order_id=1, side=None, price=0.0, volume=0).to_dict(),
        ]
    )

    in_path = tmp_path / "Events.parquet"
    out_path = tmp_path / "Snapshot.parquet"
    df.to_parquet(in_path, index=False)

    ctx = _Ctx(mode="offline", symbol=symbol, date="2025-11-04", input_path=in_path, output_path=out_path)
    with pytest.raises(ValueError, match=r"Unknown event="):
        engine.execute(ctx)


# ======================================================
# Tests: Contract / invariants
# ======================================================
def test_engine_requires_ts_int_in_events(tmp_path: Path, engine: OrderBookRebuildEngine, symbol: str):
    # If ts is not int, NormalizedEvent.from_row may accept it depending on implementation.
    # This test asserts the contract at engine level by forcing a non-int ts into dataframe.
    df = pd.DataFrame(
        [
            {
                "ts": "not_int",
                "event": "ADD",
                "order_id": 1,
                "side": "B",
                "price": 10.0,
                "volume": 100,
                "buy_no": 0,
                "sell_no": 0,
            }
        ]
    )

    in_path = tmp_path / "Events.parquet"
    out_path = tmp_path / "Snapshot.parquet"
    df.to_parquet(in_path, index=False)

    ctx = _Ctx(mode="offline", symbol=symbol, date="2025-11-04", input_path=in_path, output_path=out_path)

    # Depending on NormalizedEvent.from_row, this may raise TypeError/ValueError.
    with pytest.raises(Exception):
        engine.execute(ctx)


def test_iter_events_yields_normalized_event(events_df: pd.DataFrame):
    # Directly test _iter_events contract: it yields NormalizedEvent instances.
    got = list(OrderBookRebuildEngine._iter_events(events_df))
    assert got
    assert all(isinstance(x, NormalizedEvent) for x in got)
    assert got[0].event in {"ADD", "CANCEL", "TRADE"}
