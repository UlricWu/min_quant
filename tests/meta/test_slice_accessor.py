# tests/meta/test_slice_accessor.py
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.meta.slice_accessor import SliceAccessor
from src.meta.slice_capability import SliceCapability


class DummySliceCapability:
    """最小 capability mock，用于测试"""

    def __init__(self, index):
        self._index = index

    def keys(self):
        return list(self._index.keys())

    def bounds(self, key):
        return self._index[key]


def test_slice_accessor_basic(tmp_path: Path):
    parquet_file = tmp_path / "data.parquet"

    table = pa.table(
        {
            "symbol": ["A", "A", "B", "B", "B"],
            "price": [1, 2, 3, 4, 5],
        }
    )
    pq.write_table(table, parquet_file)

    cap = DummySliceCapability(
        {
            "A": (0, 2),
            "B": (2, 3),
        }
    )

    accessor = SliceAccessor(
        parquet_file=parquet_file,
        capability=cap,
    )

    assert set(accessor.keys()) == {"A", "B"}

    a = accessor.get("A")
    b = accessor.get("B")

    assert a.num_rows == 2
    assert b.num_rows == 3

    assert a["price"].to_pylist() == [1, 2]
    assert b["price"].to_pylist() == [3, 4, 5]
