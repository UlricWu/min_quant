#!filepath: tests/engines/test_minute_order_agg_engine.py
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from src.engines.minute_order_agg_engine import MinuteOrderAggEngine, NS_PER_MINUTE
from src.pipeline.context import EngineContext

from tests.conftest import assert_table_schema_exact, table_to_dict


def _expected_schema(include_order_count: bool) -> pa.Schema:
    fields = [
        pa.field("minute", pa.timestamp("ns")),
        pa.field("add_volume", pa.int64()),
        pa.field("cancel_volume", pa.int64()),
        pa.field("net_volume", pa.int64()),
        pa.field("add_notional", pa.float64()),
        pa.field("cancel_notional", pa.float64()),
    ]
    if include_order_count:
        fields.append(pa.field("order_count", pa.int64()))
    return pa.schema(fields)


def test_minute_order_agg_basic_contract_and_values(tmp_path: Path, pqio) -> None:
    base_ts = 1_700_000_000_000_000_000  # ns (positive)

    # NOTE: deliberately shuffled minutes to validate output sorting
    table = pa.table(
        {
            "ts": [
                base_ts + NS_PER_MINUTE + 5,  # minute 1
                base_ts + 10,                 # minute 0
                base_ts + 20,                 # minute 0
                base_ts + NS_PER_MINUTE + 9,  # minute 1
            ],
            "event": ["ADD", "ADD", "CANCEL", "CANCEL"],
            "volume": [200, 100, 40, 50],
            "signed_volume": [200, 100, -40, -50],
            "notional": [2200.0, 1000.0, 400.0, 550.0],
        }
    )

    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"
    pqio.write(in_path, table)

    engine = MinuteOrderAggEngine()
    engine.execute(
        EngineContext(mode="offline", input_path=in_path, output_path=out_path)
    )

    out = pqio.read(out_path)
    assert_table_schema_exact(out, _expected_schema(include_order_count=True))
    assert out.num_rows == 2

    d = table_to_dict(out)

    # Sorted by minute ascending => row 0 is minute 0, row 1 is minute 1
    # minute 0: ADD 100, CANCEL 40
    assert d["add_volume"][0] == 100
    assert d["cancel_volume"][0] == 40
    assert d["net_volume"][0] == 60
    assert d["add_notional"][0] == 1000.0
    assert d["cancel_notional"][0] == 400.0
    assert d["order_count"][0] == 2

    # minute 1: ADD 200, CANCEL 50
    assert d["add_volume"][1] == 200
    assert d["cancel_volume"][1] == 50
    assert d["net_volume"][1] == 150
    assert d["add_notional"][1] == 2200.0
    assert d["cancel_notional"][1] == 550.0
    assert d["order_count"][1] == 2

    # minute is timestamp[ns]
    assert pa.types.is_timestamp(out.schema.field("minute").type)


def test_minute_order_agg_trade_only_writes_empty_but_schema(tmp_path: Path, pqio) -> None:
    table = pa.table(
        {
            "ts": [1, 2, 3],
            "event": ["TRADE", "TRADE", "TRADE"],
            "volume": [100, 200, 300],
            "signed_volume": [100, -200, 300],
            "notional": [1000.0, 2000.0, 3000.0],
        }
    )

    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"
    pqio.write(in_path, table)

    engine = MinuteOrderAggEngine()
    engine.execute(
        EngineContext(mode="offline", input_path=in_path, output_path=out_path)
    )

    out = pqio.read(out_path)
    assert_table_schema_exact(out, _expected_schema(include_order_count=True))
    assert out.num_rows == 0


def test_minute_order_agg_empty_input_writes_empty_but_schema(tmp_path: Path, pqio) -> None:
    table = pa.table(
        {
            "ts": pa.array([], pa.int64()),
            "event": pa.array([], pa.string()),
            "volume": pa.array([], pa.int64()),
            "signed_volume": pa.array([], pa.int64()),
            "notional": pa.array([], pa.float64()),
        }
    )

    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"
    pqio.write(in_path, table)

    engine = MinuteOrderAggEngine()
    engine.execute(
        EngineContext(mode="offline", input_path=in_path, output_path=out_path)
    )

    out = pqio.read(out_path)
    assert_table_schema_exact(out, _expected_schema(include_order_count=True))
    assert out.num_rows == 0


def test_minute_order_agg_missing_required_columns_fails_fast(tmp_path: Path, pqio) -> None:
    # missing signed_volume, notional
    table = pa.table(
        {
            "ts": [1, 2],
            "event": ["ADD", "CANCEL"],
            "volume": [100, 50],
        }
    )

    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"
    pqio.write(in_path, table)

    engine = MinuteOrderAggEngine()

    with pytest.raises(KeyError):
        engine.execute(
            EngineContext(mode="offline", input_path=in_path, output_path=out_path)
        )


def test_minute_order_agg_disable_order_count_contract(tmp_path: Path, pqio) -> None:
    table = pa.table(
        {
            "ts": [1],
            "event": ["ADD"],
            "volume": [100],
            "signed_volume": [100],
            "notional": [1000.0],
        }
    )

    in_path = tmp_path / "orderbook_events.parquet"
    out_path = tmp_path / "minute_order.parquet"
    pqio.write(in_path, table)

    engine = MinuteOrderAggEngine(cfg=engine_cfg_no_count())
    engine.execute(
        EngineContext(mode="offline", input_path=in_path, output_path=out_path)
    )

    out = pqio.read(out_path)
    assert_table_schema_exact(out, _expected_schema(include_order_count=False))
    assert "order_count" not in out.schema.names


def engine_cfg_no_count():
    from src.engines.minute_order_agg_engine import MinuteOrderAggConfig
    return MinuteOrderAggConfig(include_order_count=False)
