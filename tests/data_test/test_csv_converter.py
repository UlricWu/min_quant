#!filepath: tests/data_test/test_csv_converter.py
import csv
from pathlib import Path

import pyarrow.parquet as pq

from src.data.csv_converter import CSVToParquetConverter
from src.utils.path import PathManager


def write_csv(path: Path, rows):
    """辅助函数：快速写 CSV"""
    with path.open("w", newline="") as f:
        writer = csv.writer(f)
        for row in rows:
            writer.writerow(row)


def test_basic_csv_to_parquet(tmp_path):
    """
    最基本情况：小 CSV → parquet
    """
    PathManager.set_root(tmp_path)

    # 准备 CSV
    csv_path = tmp_path / "a.csv"
    write_csv(csv_path, [
        ["symbol", "price"],
        ["AAPL", "100"],
        ["TSLA", "200"]
    ])

    # 输出目录
    out_dir = tmp_path / "parquet"

    converter = CSVToParquetConverter()
    parquet_path = converter.convert(csv_path, relative_dir=str(out_dir))

    # 验证 parquet 存在
    assert parquet_path.exists()

    # 读取 parquet 验证内容
    table = pq.read_table(parquet_path)
    assert table.num_rows == 2
    assert table.num_columns == 2
    assert table.column("symbol").to_pylist() == ["AAPL", "TSLA"]
    assert table.column("price").to_pylist() == [100, 200]



def test_output_dir_auto_created(tmp_path):
    """
    输出目录不存在 → 自动创建
    """
    PathManager.set_root(tmp_path)

    csv_path = tmp_path / "b.csv"
    write_csv(csv_path, [
        ["symbol", "volume"],
        ["MSFT", "3000"]
    ])

    out_dir = tmp_path / "nested" / "parquet_output"

    converter = CSVToParquetConverter()
    parquet_path = converter.convert(csv_path, relative_dir=str(out_dir))

    assert parquet_path.exists()
    assert out_dir.exists()


def test_large_csv(tmp_path):
    """
    测试大 CSV（伪造 50 万行）性能和正确性
    """
    PathManager.set_root(tmp_path)

    csv_path = tmp_path / "large.csv"

    # 写入 50 万行伪数据
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol", "price"])
        for i in range(500_000):
            writer.writerow([f"S{i}", str(i)])

    out_dir = tmp_path / "output_large"
    converter = CSVToParquetConverter()
    parquet_path = converter.convert(csv_path, relative_dir=str(out_dir))

    assert parquet_path.exists()

    # 检查 parquet 行数（不可全读入内存，可以 chunk 读）
    table = pq.read_table(parquet_path)
    assert table.num_rows == 500_000


def test_empty_csv(tmp_path):
    """
    空 CSV（只有列名）应该正常转换
    """
    PathManager.set_root(tmp_path)

    csv_path = tmp_path / "empty.csv"
    write_csv(csv_path, [
        ["symbol", "price"]
    ])

    out_dir = tmp_path / "empty_out"

    converter = CSVToParquetConverter()
    parquet_path = converter.convert(csv_path, relative_dir=str(out_dir))

    table = pq.read_table(parquet_path)
    assert table.num_rows == 0
    assert table.column_names == ["symbol", "price"]


def test_csv_with_extra_spaces(tmp_path):
    """
    测试数据中包含空格，不应被解析为 NA
    """
    PathManager.set_root(tmp_path)

    csv_path = tmp_path / "spaces.csv"
    write_csv(csv_path, [
        ["symbol", "price"],
        ["ABC ", " 100"],   # 有空格
    ])

    out_dir = tmp_path / "parquet_spaces"
    converter = CSVToParquetConverter()
    parquet_path = converter.convert(csv_path, relative_dir=str(out_dir))

    table = pq.read_table(parquet_path)

    assert table.column("symbol").to_pylist() == ["ABC "]
    assert table.column("price").to_pylist() == [100]
