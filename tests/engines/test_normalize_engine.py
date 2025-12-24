from __future__ import annotations

from pathlib import Path
from typing import Dict, Tuple

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.normalize_engine import NormalizeEngine
from src.engines.parser_engine import INTERNAL_SCHEMA
import pyarrow.compute as pc
@pytest.fixture
def sh_trade_parquet(tmp_path: Path) -> Path:
    """
    严格按 EXCHANGE_REGISTRY['sh']['trade'] 构造最小 parquet：
      - 含多 symbol + 乱序 TickTime
      - 含非 A 股（应被过滤）
    """
    table = pa.table(
        {
            "SecurityID": [
                "600000",  # A股
                "600000",
                "000001",  # A股
                "AAPL",    # 非A股 -> 必须被 filter_a_share_arrow 过滤
                "300001",  # A股
            ],

            "TradeTime": [
                2025120193002000,
                2025120193001000,
                2025120193003000,
                2025120193002000,
                2025120193002001,
            ],
            "TickTime": [
                93002000,  # 故意乱序：先 02
                93001000,  # 再 01
                93003000,
                93001000,
                93002000,
            ],
            "TickType": [
                "T", "T", "T", "T", "T"
            ],
            "Price": [
                10.1, 10.0, 9.9, 100.0, 20.0
            ],
            "Volume": [
                200, 100, 150, 10, 300
            ],
            "Side": [
                "2", "1", "1", "1", "2"
            ],
            "SubSeq": [
                2, 1, 3, 4, 5
            ],
            "BuyNo": [
                20, 10, 30, 40, 50
            ],
            "SellNo": [
                21, 11, 31, 41, 51
            ],
        }
    )

    path = tmp_path / "sh_trade.parquet"
    pq.write_table(table, path)
    return path

def test_normalize_basic(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)

    assert result.input_file == sh_trade_parquet
    assert result.output_file.exists()
    assert result.output_file.name == sh_trade_parquet.name
    assert result.rows >= 0
    assert isinstance(result.index, dict)

def test_normalize_output_schema_is_internal(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)
    table = pq.read_table(result.output_file)

    assert table.schema == INTERNAL_SCHEMA
    assert table.num_rows == result.rows
def test_normalize_filters_non_a_share(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)
    table = pq.read_table(result.output_file)

    symbols = set(table["symbol"].to_pylist())
    assert "AAPL" not in symbols
def test_normalize_sorted_by_symbol_then_ts(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)
    table = pq.read_table(result.output_file)

    symbols = table["symbol"].to_pylist()
    ts = table["ts"].to_pylist()

    for i in range(1, len(symbols)):
        if symbols[i] == symbols[i - 1]:
            assert ts[i] >= ts[i - 1]
        else:
            assert symbols[i] > symbols[i - 1]
def test_index_covers_all_rows(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)

    total = sum(length for _, length in result.index.values())
    assert total == result.rows
def test_index_slice_correctness(sh_trade_parquet: Path, tmp_path: Path):
    engine = NormalizeEngine()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = engine.execute(sh_trade_parquet, out_dir)
    table = pq.read_table(result.output_file)

    for symbol, (start, length) in result.index.items():
        sliced = table.slice(start, length)
        slice_symbols = set(sliced["symbol"].to_pylist())
        assert slice_symbols == {symbol}
def test_normalize_empty_after_filter(tmp_path: Path):
    table = pa.table(
        {
            "SecurityID": ["AAPL", "GOOG"],
            "TickTime": [93001000, 93002000],
            "TickType": ["T", "T"],
            "Price": [100.0, 200.0],
            "Volume": [10, 20],
            "Side": ["1", "2"],
            "SubSeq": [1, 2],
            "BuyNo": [10, 20],
            "SellNo": [11, 21],
        }
    )

    input_path = tmp_path / "sh_trade.parquet"
    pq.write_table(table, input_path)

    engine = NormalizeEngine()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    result = engine.execute(input_path, out_dir)

    assert result.rows == 0
    assert result.index == {}
    assert result.output_file.exists()

    out_table = pq.read_table(result.output_file)
    assert out_table.num_rows == 0
def test_normalize_invalid_filename_raises(tmp_path: Path):
    # 字段仍按 sh/trade 给齐，避免先在 parser 内因缺字段炸
    table = pa.table(
        {
            "SecurityID": ["600000"],
            "TickTime": [93001000],
            "TickType": ["T"],
            "Price": [10.0],
            "Volume": [100],
            "Side": ["1"],
            "SubSeq": [1],
            "BuyNo": [10],
            "SellNo": [11],
        }
    )

    bad_path = tmp_path / "SH_trade.parquet"  # ❌ exchange 大写，不在 registry（只有 'sh'）
    pq.write_table(table, bad_path)

    engine = NormalizeEngine()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with pytest.raises(KeyError):
        engine.execute(bad_path, out_dir)
def build_index_python(table: pa.Table) -> Dict[str, Tuple[int, int]]:
    symbols = table["symbol"].to_pylist()
    index = {}
    start = 0
    cur = symbols[0]
    for i in range(1, len(symbols)):
        if symbols[i] != cur:
            index[cur] = (start, i - start)
            cur = symbols[i]
            start = i
    index[cur] = (start, len(symbols) - start)
    return index


def test_build_symbol_slice_index_matches_python():
    table = pa.table({
        "symbol": ["000001","000001","000002","000002","000002","000003"],
        "ts":     [1,2,1,2,3,1],
    })

    index_arrow = NormalizeEngine.build_symbol_slice_index(table)
    index_py = build_index_python(table)

    assert index_arrow == index_py


def test_build_symbol_slice_index_semantics():
    table = pa.table({
        "symbol": ["000001","000001","000002","000002","000003"],
        "ts":     [1,2,1,2,1],
    })

    index = NormalizeEngine.build_symbol_slice_index(table)

    for sym, (start, length) in index.items():
        slice_sym = table["symbol"].slice(start, length)
        assert pc.all(pc.equal(slice_sym, sym)).as_py()

