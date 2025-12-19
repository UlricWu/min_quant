import pyarrow as pa

from src.engines.event_splitter_engine import TickTypeSplitterEngine


def test_splitter_basic_split():
    engine = TickTypeSplitterEngine()

    batch = pa.record_batch(
        [
            pa.array(["A", "T", "M", "T"]),
            pa.array(["1", "2", "3", "4"]),
        ],
        names=["TickType", "col"],
    )

    order_batch, trade_batch = engine.split(batch)

    assert order_batch.num_rows == 2
    assert trade_batch.num_rows == 2

    assert order_batch.column(0).to_pylist() == ["A", "M"]
    assert trade_batch.column(0).to_pylist() == ["T", "T"]

def test_splitter_preserves_schema():
    engine = TickTypeSplitterEngine()

    batch = pa.record_batch(
        [
            pa.array(["A", "T"]),
            pa.array(["x", "y"]),
        ],
        names=["TickType", "foo"],
    )

    order_batch, trade_batch = engine.split(batch)

    assert order_batch.schema == batch.schema
    assert trade_batch.schema == batch.schema

def test_splitter_preserves_order():
    engine = TickTypeSplitterEngine()

    batch = pa.record_batch(
        [
            pa.array(["T", "A", "T", "M"]),
            pa.array(["1", "2", "3", "4"]),
        ],
        names=["TickType", "col"],
    )

    order_batch, trade_batch = engine.split(batch)

    assert order_batch.column(1).to_pylist() == ["2", "4"]
    assert trade_batch.column(1).to_pylist() == ["1", "3"]
import pytest

def test_splitter_missing_ticktype_column():
    engine = TickTypeSplitterEngine()

    batch = pa.record_batch(
        [pa.array(["1", "2"])],
        names=["col"],
    )

    with pytest.raises(ValueError):
        engine.split(batch)
