#!filepath: tests/test_pipeline.py
import pytest
from pathlib import Path

from src.data.pipeline import DataPipeline


# ---------------------------------------------------------
@pytest.fixture
def setup_env(tmp_path, monkeypatch):

    class PM:
        def raw_dir(self): return tmp_path / "raw"
        def parquet_dir(self): return tmp_path / "parquet"
        def temp_dir(self): return tmp_path / "tmp"

    # mock PathManager
    monkeypatch.setattr("src.data.pipeline.PathManager", PM)

    # ---- mock 各部件（只记录调用，不做实际处理） ----
    class DummyDownloader:
        called = False
        def download(self, date):
            self.called = True
            (tmp_path / "raw" / date).mkdir(parents=True)

    class DummyDecompressor:
        called = False
        def extract_7z(self, zfile, out_dir):
            self.called = True
            p = Path(out_dir)
            p.mkdir(parents=True, exist_ok=True)  # ★ 修复点
            # 写入 tmp csv
            (Path(out_dir) / (zfile.stem + ".csv")).write_text("col\n1")

    class DummyConverter:
        called = False
        def convert(self, csv_file, relative_dir):
            self.called = True
            p = Path(relative_dir)
            p.mkdir(parents=True, exist_ok=True)  # ★ 修复点
            (p / (csv_file.stem + ".parquet")).write_text("parquet")

    class DummySHC:
        called = False
        def split(self, parquet_file):
            self.called = True
            # 模拟生成 SH_order / SH_trade
            base = parquet_file.parent
            (base / "SH_order.parquet").write_text("order")
            (base / "SH_trade.parquet").write_text("trade")

    class DummyRouter:
        called = False
        def __init__(self, x=None): pass
        def route_date(self, date):
            self.called = True

    monkeypatch.setattr("src.data.pipeline.FTPDownloader", lambda: DummyDownloader())
    monkeypatch.setattr("src.data.pipeline.Decompressor", lambda: DummyDecompressor())
    monkeypatch.setattr("src.data.pipeline.CSVToParquetConverter", lambda: DummyConverter())
    monkeypatch.setattr("src.data.pipeline.ShConverter", lambda: DummySHC())
    monkeypatch.setattr("src.data.pipeline.SymbolRouter", lambda x: DummyRouter())

    return tmp_path


# ---------------------------------------------------------
def test_pipeline_run(setup_env):
    tmp_path = setup_env
    date = "20250101"

    raw_dir = tmp_path / "raw" / date
    raw_dir.mkdir(parents=True)

    # 模拟存在 7z 文件
    zfile = raw_dir / "SH_test.7z"
    zfile.write_text("dummy")

    pipeline = DataPipeline()
    pipeline.run(date)

    # 原 parquet 文件应该被删除
    parquet_file = tmp_path / "parquet" / date / "SH_test.parquet"
    assert not parquet_file.exists()  # ✔ pipeline 会删除

    # SH 拆分文件必须存在
    order_file = tmp_path / "parquet" / date / "SH_order.parquet"
    trade_file = tmp_path / "parquet" / date / "SH_trade.parquet"

    assert order_file.exists()
    assert trade_file.exists()

