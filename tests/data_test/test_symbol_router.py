#!filepath: tests/data_test/test_symbol_router.py


import json
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
    sym_600000 = tmp_path / "data" / "symbol" / "600000" / f"{DATE}.parquet"
    sym_000001 = tmp_path / "data" / "symbol" / "000001" / f"{DATE}.parquet"
    sym_300750 = tmp_path / "data" / "symbol" / "300750" / f"{DATE}.parquet"

    assert sym_600000.exists()
    assert sym_000001.exists()
    assert sym_300750.exists()

    # 检查非 0/3/6 前缀的 123456 被过滤
    assert not (tmp_path / "data/symbol/123456.SH").exists()

    # 校验内容
    t = pq.read_table(sym_600000)
    assert t.column("SecurityID").to_pylist() == ["600000"]


def setup_dirs(tmp_path):
    """初始化标准目录结构"""
    PathManager.set_root(tmp_path)

    parquet_dir = tmp_path / "data" / "parquet" / DATE
    symbol_dir = tmp_path / "data" / "symbol"
    metadata_dir = tmp_path / "data" / "metadata"

    parquet_dir.mkdir(parents=True, exist_ok=True)
    symbol_dir.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)

    return parquet_dir, symbol_dir, metadata_dir


def create_test_parquet(parquet_dir: Path):
    """创建一个测试 parquet，模拟真实 level2 输出"""

    table = pa.table({
        "SecurityID": ["600000", "000001", "300750", "123456"],  # 最后一个应被过滤
        "price": [10, 20, 30, 40],
    })

    file_path = parquet_dir / "test_data.parquet"
    pq.write_table(table, file_path)

    return file_path, table


# -------------------------------------------------------------------
#                         E2E 测试（无 pipeline）
# -------------------------------------------------------------------
def test_symbol_router_e2e(tmp_path):
    """完整测试：拆分 symbol → 写 parquet → 写 metadata"""

    parquet_dir, symbol_dir, metadata_dir = setup_dirs(tmp_path)

    # 1) 准备一个输入 parquet
    parquet_path, table = create_test_parquet(parquet_dir)

    # 2) 初始化 router
    router = SymbolRouter()

    # 3) 运行 router
    router.route_date(DATE)

    # -------------------------------------------------------------------
    # 4) 检查 symbol parquet 是否生成
    # -------------------------------------------------------------------

    out_600000 = symbol_dir / "600000" / f"{DATE}.parquet"
    out_000001 = symbol_dir / "000001" / f"{DATE}.parquet"
    out_300750 = symbol_dir / "300750" / f"{DATE}.parquet"
    out_123456 = symbol_dir / "123456" / f"{DATE}.parquet"

    assert out_600000.exists(), "600000 parquet 未生成"
    assert out_000001.exists(), "000001 parquet 未生成"
    assert out_300750.exists(), "300750 parquet 未生成"
    assert not out_123456.exists(), "123456 不应生成（非法 symbol）"

    # -------------------------------------------------------------------
    # 5) 检查 parquet 内容
    # -------------------------------------------------------------------

    t600000 = pq.read_table(out_600000)
    assert t600000.column("SecurityID").to_pylist() == ["600000"]

    t000001 = pq.read_table(out_000001)
    assert t000001.column("SecurityID").to_pylist() == ["000001"]

    # -------------------------------------------------------------------
    # 6) 检查 metadata JSON
    # -------------------------------------------------------------------

    meta_json_path = metadata_dir / f"{DATE}.json"
    assert meta_json_path.exists(), "metadata JSON 未生成"

    meta = json.loads(meta_json_path.read_text())

    # 输入文件
    assert len(meta["input_files"]) == 1
    assert meta["input_files"][0]["file"] == "test_data.parquet"

    # 输出 symbol
    symbols = [s["symbol"] for s in meta["symbols"]]
    assert "600000" in symbols
    assert "000001" in symbols
    assert "300750" in symbols

    # 非法 symbol
    assert "123456" in meta["filtered_symbols"]

    # 数量
    assert meta["symbols_count"] == 3
    assert meta["rows_total"] == 3, "三只股票各 1 行，应为 3 行"
