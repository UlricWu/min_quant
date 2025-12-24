from __future__ import annotations

import pytest
import pyarrow as pa

from src.engines.parser_engine import (
    parse_events_arrow,
    INTERNAL_SCHEMA,
)


def test_parse_empty_table():
    table = pa.table({})
    out = parse_events_arrow(table, exchange="sh", kind="trade")
    assert out.num_rows == 0
def test_parse_invalid_exchange():
    table = pa.table({"a": [1]})
    with pytest.raises(KeyError):
        parse_events_arrow(table, exchange="xx", kind="trade")
def test_parse_invalid_kind():
    table = pa.table({"a": [1]})
    with pytest.raises(KeyError):
        parse_events_arrow(table, exchange="sh", kind="xxx")
@pytest.fixture
def sh_trade_table():
    return pa.table(
        {
            "SecurityID": ["600000", "600000"],
            "TickTime": [2025010193001000, 2025010193002000],   # ✅ time_field
            "TickType": ["T", "T"],             # ✅ event_field
            "Price": [10.0, 10.1],
            "Volume": [100, 200],
            "Side": ["1", "2"],
            "SubSeq": [1, 2],
            "BuyNo": [10, 20],
            "SellNo": [11, 21],
        }
    )


def test_parse_sh_trade_basic(sh_trade_table):
    out = parse_events_arrow(
        sh_trade_table,
        exchange="sh",
        kind="trade",
    )

    assert out.schema == INTERNAL_SCHEMA
    assert out.num_rows == 2
def test_parse_event_mapping(sh_trade_table):
    out = parse_events_arrow(
        sh_trade_table,
        exchange="sh",
        kind="trade",
    )

    events = out["event"].to_pylist()
    assert events == ["TRADE", "TRADE"]
def test_parse_side_mapping(sh_trade_table):
    out = parse_events_arrow(
        sh_trade_table,
        exchange="sh",
        kind="trade",
    )

    sides = out["side"].to_pylist()
    assert sides == ["B", "S"]
def test_parse_buy_sell_no(sh_trade_table):
    out = parse_events_arrow(
        sh_trade_table,
        exchange="sh",
        kind="trade",
    )

    assert out["buy_no"].to_pylist() == [10, 20]
    assert out["sell_no"].to_pylist() == [11, 21]
def test_parse_ts_monotonic(sh_trade_table):
    out = parse_events_arrow(
        sh_trade_table,
        exchange="sh",
        kind="trade",
    )

    ts = out["ts"].to_pylist()
    assert ts[0] < ts[1]
@pytest.fixture
def sz_trade_table():
    return pa.table(
        {
            "SecurityID": ["000001"],
            "TickTime": [2025010193001000],
            "ExecType": ["1"],  # TRADE
            "TradePrice": [12.5],
            "TradeVolume": [300],
            "SubSeq": [99],
            "BuyNo": [100],
            "SellNo": [200],
        }
    )
def test_parse_sz_trade_side_is_null(sz_trade_table):
    out = parse_events_arrow(
        sz_trade_table,
        exchange="sz",
        kind="trade",
    )

    assert out["side"].null_count == 1
def test_internal_schema_frozen(sh_trade_table):
    out = parse_events_arrow(
        sh_trade_table,
        exchange="sh",
        kind="trade",
    )

    assert out.schema == INTERNAL_SCHEMA
