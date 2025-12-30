# tests/meta/test_slice_source.py
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.meta.base import BaseMeta, MetaOutput
from src.meta.slice_source import SliceSource


def test_slice_source_iter_tables(tmp_path: Path):
    meta_dir = tmp_path / "meta"
    fact_dir = tmp_path / "fact"
    meta_dir.mkdir()
    fact_dir.mkdir()

    # 构造 parquet
    table = pa.table(
        {
            "symbol": ["A", "A", "B"],
            "price": [10, 11, 20],
        }
    )
    parquet_file = fact_dir / "sh_trade.normalize.parquet"
    pq.write_table(table, parquet_file)

    # 写 manifest（带 index）
    meta = BaseMeta(
        meta_dir=meta_dir,
        stage="normalize",
        output_slot="sh_trade",
    )
    meta.commit(
        MetaOutput(
            input_file=parquet_file,
            output_file=parquet_file,
            rows=3,
            index={
                "A": (0, 2),
                "B": (2, 1),
            },
        )
    )

    # -------- 核心测试点 --------
    source = SliceSource(
        meta_dir=meta_dir,
        stage="normalize",
        output_slot="sh_trade",
    )

    results = list(source)

    assert len(results) == 2

    sym_a, tbl_a = results[0]
    sym_b, tbl_b = results[1]

    assert sym_a == "A"
    assert sym_b == "B"

    assert tbl_a["price"].to_pylist() == [10, 11]
    assert tbl_b["price"].to_pylist() == [20]


def test_slice_source_get(tmp_path: Path):
    meta_dir = tmp_path / "meta"
    fact_dir = tmp_path / "fact"
    meta_dir.mkdir()
    fact_dir.mkdir()

    table = pa.table(
        {
            "symbol": ["X", "X", "Y"],
            "price": [1, 2, 3],
        }
    )
    parquet_file = fact_dir / "data.normalize.parquet"
    pq.write_table(table, parquet_file)

    meta = BaseMeta(meta_dir, stage="normalize", output_slot="data")
    meta.commit(
        MetaOutput(
            input_file=parquet_file,
            output_file=parquet_file,
            rows=3,
            index={
                "X": (0, 2),
                "Y": (2, 1),
            },
        )
    )

    source = SliceSource(
        meta_dir=meta_dir,
        stage="normalize",
        output_slot="data",
    )

    x = source.get("X")
    y = source.get("Y")

    assert x.num_rows == 2
    assert y.num_rows == 1
