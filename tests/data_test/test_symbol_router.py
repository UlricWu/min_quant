#!filepath: tests/data_test/test_symbol_router.py
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from src.data.symbol_router import SymbolRouter
from src.utils.path import PathManager


DATE = "2025-11-23"


def test_symbol_router_basic(tmp_path):
    PathManager.set_root(tmp_path)

    # 创建 input parquet
    date_dir = tmp_path / "data" / "parquet" / DATE
    date_dir.mkdir(parents=True)

    table = pa.table({
        "SecurityID": ["600000", "000001", "300750", "123456"],  # 最后一个无效
        "price": [10, 20, 30, 40],
    })

    pq.write_table(table, date_dir / "test.parquet")

    # 运行 router
    router = SymbolRouter()
    router.route_date(DATE)

    # 检查输出
    sym_600000 = tmp_path / "data" / "symbol" / "600000.SH" / f"{DATE}.parquet"
    sym_000001 = tmp_path / "data" / "symbol" / "000001.SZ" / f"{DATE}.parquet"
    sym_300750 = tmp_path / "data" / "symbol" / "300750.SZ" / f"{DATE}.parquet"

    assert sym_600000.exists()
    assert sym_000001.exists()
    assert sym_300750.exists()

    # 检查非 0/3/6 前缀的 123456 被过滤
    assert not (tmp_path / "data/symbol/123456.SH").exists()

    # 校验内容
    t = pq.read_table(sym_600000)
    assert t.column("SecurityID").to_pylist() == ["600000"]
