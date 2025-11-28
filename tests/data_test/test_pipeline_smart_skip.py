#!filepath: tests/data_test/test_pipeline_e2e.py
from pathlib import Path
from unittest.mock import patch

import pytest

from src.data.pipeline import DataPipeline
from src.utils.path import PathManager


DATE = "2025-11-23"


def setup_paths(tmp_path: Path):
    """
    帮你统一创建 pipeline 中用到的路径：
      data/raw/<date>/
      data/parquet/<date>/
      tmp/decompress/<date>/
    然后通过 PathManager.set_root 指向 tmp_path
    """
    PathManager.set_root(tmp_path)

    raw_dir = tmp_path / "data" / "raw" / DATE
    parquet_dir = tmp_path / "data" / "parquet" / DATE
    tmp_root = tmp_path / "tmp" / "decompress" / DATE

    raw_dir.mkdir(parents=True, exist_ok=True)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    tmp_root.mkdir(parents=True, exist_ok=True)

    return raw_dir, parquet_dir, tmp_root


# ---------------------------------------------------------
# RULE 1: 如果 data/raw 存在 .7z，则跳过下载
# ---------------------------------------------------------
def test_raw_exists_skip_download(tmp_path):
    raw_dir, parquet_dir, tmp_root = setup_paths(tmp_path)

    # 创建一个已有的 7z 文件
    zfile = raw_dir / "a.csv.7z"
    zfile.write_bytes(b"dummy")

    with patch("src.data.pipeline.FTPDownloader") as mock_dl, \
         patch("src.data.pipeline.Decompressor") as mock_dc, \
         patch("src.data.pipeline.CSVToParquetConverter") as mock_conv:

        downloader = mock_dl.return_value
        decompressor = mock_dc.return_value
        converter = mock_conv.return_value

        pipeline = DataPipeline()
        pipeline.run(DATE)

        # raw 中已经有 7z ⇒ 不应该调用 download
        downloader.download.assert_not_called()

        # 仍然会继续后续流程（这里不检查细节）
        # 只要 pipeline 跑完不报错即可
        assert True


# ---------------------------------------------------------
# RULE 1: 如果 data/raw 为空，则会调用下载
# ---------------------------------------------------------
def test_raw_missing_triggers_download(tmp_path):
    raw_dir, parquet_dir, tmp_root = setup_paths(tmp_path)

    # raw_dir 为空，不创建任何 .7z

    with patch("src.data.pipeline.FTPDownloader") as mock_dl, \
         patch("src.data.pipeline.Decompressor"), \
         patch("src.data.pipeline.CSVToParquetConverter"):

        downloader = mock_dl.return_value

        pipeline = DataPipeline()
        pipeline.run(DATE)

        # raw 中没有 7z ⇒ 应该调用 download
        downloader.download.assert_called_once_with(DATE)


# ---------------------------------------------------------
# RULE 2: 如果 parquet/<date>/a.parquet 存在，
#         则跳过解压 & 转换（对该文件）
# ---------------------------------------------------------
def test_parquet_exists_skip_decompress_and_convert(tmp_path):
    raw_dir, parquet_dir, tmp_root = setup_paths(tmp_path)

    # raw 中有 a.csv.7z
    zfile = raw_dir / "a.csv.7z"
    zfile.write_bytes(b"dummy")

    # parquet 中已经有 a.parquet
    a_parquet = parquet_dir / "a.parquet"
    a_parquet.write_bytes(b"parquet-dummy")

    with patch("src.data.pipeline.FTPDownloader") as mock_dl, \
         patch("src.data.pipeline.Decompressor") as mock_dc, \
         patch("src.data.pipeline.CSVToParquetConverter") as mock_conv:

        downloader = mock_dl.return_value
        decompressor = mock_dc.return_value
        converter = mock_conv.return_value

        pipeline = DataPipeline()
        pipeline.run(DATE)

        # raw 已存在 ⇒ 不下载
        downloader.download.assert_not_called()
        # parquet 已存在 ⇒ 不解压 & 不转换
        decompressor.extract_7z.assert_not_called()
        converter.convert.assert_not_called()


# ---------------------------------------------------------
# RULE 3: parquet 不存在 & tmp/<date>/a.csv 存在，
#         跳过解压，只做 csv → parquet
# ---------------------------------------------------------
def test_tmp_csv_exists_skip_decompress_but_convert(tmp_path):
    raw_dir, parquet_dir, tmp_root = setup_paths(tmp_path)

    # raw 中有 a.csv.7z
    zfile = raw_dir / "a.csv.7z"
    zfile.write_bytes(b"dummy")

    # parquet 中还没有 a.parquet

    # tmp 中提前放置 a.csv，模拟“上次已解压，只是没转 parquet”
    tmp_csv_path = tmp_root / "a.csv"
    tmp_csv_path.write_text("symbol,price\nAAPL,100")

    with patch("src.data.pipeline.FTPDownloader") as mock_dl, \
         patch("src.data.pipeline.Decompressor") as mock_dc, \
         patch("src.data.pipeline.CSVToParquetConverter") as mock_conv:

        downloader = mock_dl.return_value
        decompressor = mock_dc.return_value
        converter = mock_conv.return_value

        pipeline = DataPipeline()
        pipeline.run(DATE)

        downloader.download.assert_not_called()
        # tmp 有 a.csv ⇒ 不应解压
        decompressor.extract_7z.assert_not_called()

        # 应该直接从 tmp_csv_path 转换
        converter.convert.assert_called_once()

        called_args, called_kwargs = converter.convert.call_args
        assert called_args[0] == tmp_csv_path
        # 第二个参数是 relative_dir（传的是 str(parquet_root)）
        assert "relative_dir" in called_kwargs
        assert called_kwargs["relative_dir"] == str(parquet_dir)


# ---------------------------------------------------------
# RULE 3 未命中：parquet 不存在 & tmp/a.csv 也不存在
#         ⇒ 需要调用解压 + 转换
# ---------------------------------------------------------
def test_no_tmp_triggers_decompress_and_convert(tmp_path):
    raw_dir, parquet_dir, tmp_root = setup_paths(tmp_path)

    # raw 中有 a.csv.7z
    zfile = raw_dir / "a.csv.7z"
    zfile.write_bytes(b"dummy")

    # parquet 中还没有 a.parquet
    # tmp 中也没有 a.csv

    with patch("src.data.pipeline.FTPDownloader") as mock_dl, \
         patch("src.data.pipeline.Decompressor") as mock_dc, \
         patch("src.data.pipeline.CSVToParquetConverter") as mock_conv:

        downloader = mock_dl.return_value
        decompressor = mock_dc.return_value
        converter = mock_conv.return_value

        pipeline = DataPipeline()
        pipeline.run(DATE)

        downloader.download.assert_not_called()

        # 应该调用解压一次
        decompressor.extract_7z.assert_called_once()
        # 然后调用一次转换（传入的第一个参数是 tmp_root 下的 a.csv）
        converter.convert.assert_called_once()

        called_args, called_kwargs = converter.convert.call_args
        tmp_csv_path = called_args[0]
        assert tmp_csv_path.name == "a.csv"
        assert "relative_dir" in called_kwargs
        assert called_kwargs["relative_dir"] == str(parquet_dir)
