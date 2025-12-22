from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.engines.normalize_engine import NormalizeEngine
from src.engines.parser_engine import INTERNAL_SCHEMA


# ------------------------------------------------------------
# helper: 写“交易所级原始 parquet”
# ------------------------------------------------------------
def write_raw_exchange_parquet(path: Path, rows, schema):
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


# ============================================================
# 1. A 股过滤（SecurityID 前缀）
# ============================================================
def test_normalize_filter_a_share(tmp_path):
    in_path = tmp_path / "sh_trade.parquet"
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    schema = pa.schema(
        [
            ("SecurityID", pa.string()),
            ("TradeTime", pa.int64()),
            ("TickTime", pa.int64()),
            ("TickType", pa.string()),
            ("Price", pa.float64()),
            ("Volume", pa.int64()),
            ("Side", pa.string()),
            ("SubSeq", pa.int64()),
            ("BuyNo", pa.int64()),
            ("SellNo", pa.int64()),
        ]
    )

    rows = [
        # A 股
        {
            "SecurityID": "600000",
            "TradeTime": 2025010109300000000,
            "TickTime": 93000000,
            "TickType": "T",
            "Price": 10.0,
            "Volume": 100,
            "Side": "1",
            "SubSeq": 1,
            "BuyNo": 11,
            "SellNo": 22,
        },
        # 非 A 股（应被过滤）
        {
            "SecurityID": "900000",
            "TradeTime": 2025010109300000000,
            "TickTime": 93000000,
            "TickType": "T",
            "Price": 20.0,
            "Volume": 200,
            "Side": "1",
            "SubSeq": 2,
            "BuyNo": 33,
            "SellNo": 44,
        },
    ]

    write_raw_exchange_parquet(in_path, rows, schema)

    engine = NormalizeEngine()
    engine.execute(in_path, out_dir)

    out_path = out_dir / in_path.name
    assert out_path.exists()

    table = pq.read_table(out_path)
    assert table.num_rows == 1
    assert table["symbol"].to_pylist() == ["600000"]


# ============================================================
# 2. 输出 schema 必须是 INTERNAL_SCHEMA
# ============================================================
def test_normalize_schema(tmp_path):
    in_path = tmp_path / "sh_trade.parquet"
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    schema = pa.schema(
        [
            ("SecurityID", pa.string()),
            ("TradeTime", pa.int64()),
            ("TickTime", pa.int64()),
            ("TickType", pa.string()),
            ("Price", pa.float64()),
            ("Volume", pa.int64()),
            ("Side", pa.string()),
            ("SubSeq", pa.int64()),
            ("BuyNo", pa.int64()),
            ("SellNo", pa.int64()),
        ]
    )

    rows = [
        {
            "SecurityID": "600000",
            "TradeTime": 2025010109300000000,
            "TickTime": 93000000,
            "TickType": "T",
            "Price": 10.0,
            "Volume": 100,
            "Side": "1",
            "SubSeq": 1,
            "BuyNo": 11,
            "SellNo": 22,
        },
    ]

    write_raw_exchange_parquet(in_path, rows, schema)

    engine = NormalizeEngine()
    engine.execute(in_path, out_dir)

    table = pq.read_table(out_dir / in_path.name)
    assert table.schema == INTERNAL_SCHEMA


# ============================================================
# 3. 空输入 parquet（安全）
# ============================================================
def test_normalize_empty_input(tmp_path):
    in_path = tmp_path / "sh_trade.parquet"
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    schema = pa.schema(
        [
            ("SecurityID", pa.string()),
            ("TradeTime", pa.int64()),
            ("TickTime", pa.int64()),
            ("TickType", pa.string()),
            ("Price", pa.float64()),
            ("Volume", pa.int64()),
            ("Side", pa.string()),
            ("SubSeq", pa.int64()),
            ("BuyNo", pa.int64()),
            ("SellNo", pa.int64()),
        ]
    )

    empty = pa.Table.from_arrays(
        [pa.array([], type=f.type) for f in schema],
        schema=schema,
    )
    pq.write_table(empty, in_path)

    engine = NormalizeEngine()
    engine.execute(in_path, out_dir)

    out_path = out_dir / in_path.name
    # writer never opened → no output
    assert not out_path.exists()


# ============================================================
# 4. 多 batch（batch_size 逻辑）
# ============================================================
def test_normalize_multi_batch(tmp_path):
    in_path = tmp_path / "sh_trade.parquet"
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    schema = pa.schema(
        [
            ("SecurityID", pa.string()),
            ("TradeTime", pa.int64()),
            ("TickTime", pa.int64()),
            ("TickType", pa.string()),
            ("Price", pa.float64()),
            ("Volume", pa.int64()),
            ("Side", pa.string()),
            ("SubSeq", pa.int64()),
            ("BuyNo", pa.int64()),
            ("SellNo", pa.int64()),
        ]
    )

    rows = [
        {
            "SecurityID": "600000",
            "TradeTime": 2025010109300000000,
            "TickTime": 93000000,
            "TickType": "T",
            "Price": 10.0,
            "Volume": 100,
            "Side": "1",
            "SubSeq": i,
            "BuyNo": 1,
            "SellNo": 2,
        }
        for i in range(10)
    ]

    write_raw_exchange_parquet(in_path, rows, schema)

    engine = NormalizeEngine()
    engine.batch_size = 3  # 强制多 batch
    engine.execute(in_path, out_dir)

    table = pq.read_table(out_dir / in_path.name)
    assert table.num_rows == 10
