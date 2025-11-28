#!filepath: tests/data_test/test_decompressor.py
import py7zr
from pathlib import Path

from src.data.decompressor import Decompressor
from src.utils.path import PathManager


def create_sample_7z(tmp_path: Path, nested=False) -> Path:
    """
    创建 sample.7z，用于测试。
    - nested=False: sample.csv 直接在根目录
    - nested=True : inner/sample.csv （保持目录结构）
    """
    src_dir = tmp_path / "src"
    src_dir.mkdir()

    if nested:
        inner = src_dir / "inner"
        inner.mkdir()
        csv_path = inner / "sample.csv"
    else:
        csv_path = src_dir / "sample.csv"

    csv_path.write_text("symbol,price\nAAPL,100")

    # 生成 .7z
    zfile = tmp_path / "sample.7z"
    with py7zr.SevenZipFile(zfile, "w") as archive:
        # arcname 确保内部目录结构被保留
        archive.write(csv_path, arcname=str(csv_path.relative_to(src_dir)))

    return zfile


def test_extract_simple_csv(tmp_path):
    """
    测试：无嵌套目录的 .7z 能正确解压到 out_dir/sample.csv
    """
    PathManager.set_root(tmp_path)

    zfile = create_sample_7z(tmp_path, nested=False)

    out_dir = tmp_path / "out"
    out_dir.mkdir()

    dec = Decompressor()
    dec.extract_7z(zfile, str(out_dir))

    extracted = list(out_dir.glob("*.csv"))
    assert len(extracted) == 1

    csv_file = extracted[0]
    assert csv_file.name == "sample.csv"
    assert csv_file.read_text() == "symbol,price\nAAPL,100"


def test_extract_nested_csv(tmp_path):
    """
    测试：含有目录结构的 .7z 必须完整保留结构
    """
    PathManager.set_root(tmp_path)

    zfile = create_sample_7z(tmp_path, nested=True)

    out_dir = tmp_path / "out_nested"
    out_dir.mkdir()

    dec = Decompressor()
    dec.extract_7z(zfile, str(out_dir))

    # 原结构 inner/sample.csv 必须保留
    inner_csv = out_dir / "inner" / "sample.csv"
    assert inner_csv.exists()
    assert inner_csv.read_text() == "symbol,price\nAAPL,100"


def test_extract_returns_none(tmp_path):
    """
    你的 extract_7z() 不返回任何值 → 应返回 None（保持稳定 API）
    """
    PathManager.set_root(tmp_path)

    zfile = create_sample_7z(tmp_path)

    out_dir = tmp_path / "out3"
    out_dir.mkdir()

    dec = Decompressor()
    result = dec.extract_7z(zfile, str(out_dir))

    assert result is None
