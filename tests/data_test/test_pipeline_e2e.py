# #!filepath: tests/data_test/test_pipeline_e2e.py
# from pathlib import Path
# import py7zr
# import csv
# from unittest.mock import patch
#
# import pyarrow.parquet as pq
#
# from src.data.pipeline import DataPipeline
# from src.utils.path import PathManager
#
#
# DATE = "2025-11-23"
#
#
# def create_7z_with_csv(tmp_path: Path):
#     """
#     创建 a.csv.7z，内部内容为 a.csv（扁平）
#     """
#     src_root = tmp_path / "src_data"
#     src_root.mkdir()
#
#     csv_path = src_root / "a.csv"
#     csv_path.write_text("symbol,price\nAAPL,100")
#
#     zfile_path = tmp_path / "a.csv.7z"
#     with py7zr.SevenZipFile(zfile_path, "w") as archive:
#         # 强制使用 arcname="a.csv" 确保 pipeline 能正确解析
#         archive.write(csv_path, arcname="a.csv")
#
#     return zfile_path
#
#
#
# def setup_directories(tmp_path):
#     """
#     helper: pipeline 所需目录：
#     data/raw/DATE
#     data/parquet/DATE
#     tmp/decompress/DATE
#     """
#     PathManager.set_root(tmp_path)
#
#     raw_dir = tmp_path / "data" / "raw" / DATE
#     parquet_dir = tmp_path / "data" / "parquet" / DATE
#     tmp_dir = tmp_path / "tmp" / "decompress" / DATE
#
#     raw_dir.mkdir(parents=True, exist_ok=True)
#     parquet_dir.mkdir(parents=True, exist_ok=True)
#     tmp_dir.mkdir(parents=True, exist_ok=True)
#
#     return raw_dir, parquet_dir, tmp_dir
#
#
# # --------------------------------------------------------
# # FULL PIPELINE：A.csv.7z → tmp/a.csv → parquet/a.parquet
# # --------------------------------------------------------
#
#
# # --------------------------------------------------------
# # RULE 1：raw 已存在 .7z → 跳过下载
# # --------------------------------------------------------
# def test_pipeline_skip_download_if_raw_exists(tmp_path):
#     raw_dir, parquet_dir, tmp_dir = setup_directories(tmp_path)
#
#     zfile = create_7z_with_csv(tmp_path)
#     (raw_dir / zfile.name).write_bytes(zfile.read_bytes())
#
#     with patch("src.data.pipeline.FTPDownloader") as mock_dl:
#         pipeline = DataPipeline()
#         pipeline.run(DATE)
#
#         mock_dl.return_value.download.assert_not_called()
#
#
# # --------------------------------------------------------
# # RULE 2：如果 parquet 已存在 → 跳过 解压 + 转换
# # --------------------------------------------------------
# def test_pipeline_skip_if_parquet_exists(tmp_path):
#     raw_dir, parquet_dir, tmp_dir = setup_directories(tmp_path)
#
#     # raw 目录已有 a.csv.7z
#     zfile = create_7z_with_csv(tmp_path)
#     (raw_dir / zfile.name).write_bytes(zfile.read_bytes())
#
#     # 创建现成 parquet → 应跳过整个文件
#     parquet_file = parquet_dir / "a.parquet"
#     parquet_file.write_bytes(b"dummy")
#
#     with patch("src.data.pipeline.FTPDownloader") as mock_dl, \
#          patch("src.data.pipeline.Decompressor") as mock_dc, \
#          patch("src.data.pipeline.CSVToParquetConverter") as mock_conv:
#
#         pipeline = DataPipeline()
#         pipeline.run(DATE)
#
#         # raw 存在 → 不下载
#         mock_dl.return_value.download.assert_not_called()
#
#         # parquet 存在 → 不应触发解压 & 转换
#         mock_dc.return_value.extract_7z.assert_not_called()
#         mock_conv.return_value.convert.assert_not_called()
#
#
# # --------------------------------------------------------
# # RULE 3：tmp 已存在 a.csv → 跳过解压，但必须执行 csv→parquet
# # --------------------------------------------------------
# def test_pipeline_skip_decompress_if_tmp_exists(tmp_path):
#     raw_dir, parquet_dir, tmp_dir = setup_directories(tmp_path)
#
#     # 放置 a.csv.7z
#     zfile = create_7z_with_csv(tmp_path)
#     (raw_dir / zfile.name).write_bytes(zfile.read_bytes())
#
#     # tmp 已经有 a.csv，模拟上次中断后已解压
#     tmp_csv = tmp_dir / "a.csv"
#     tmp_csv.write_text("symbol,price\nMSFT,300")
#
#     with patch("src.data.pipeline.Decompressor") as mock_dc, \
#          patch("src.data.pipeline.CSVToParquetConverter") as mock_conv:
#
#         pipeline = DataPipeline()
#         pipeline.run(DATE)
#
#         # 应跳过解压
#         mock_dc.return_value.extract_7z.assert_not_called()
#
#         # 但必须转换
#         mock_conv.return_value.convert.assert_called_once()
#
#         args, kwargs = mock_conv.return_value.convert.call_args
#         assert args[0] == tmp_csv      # 输入应是 tmp_csv
#         assert kwargs["relative_dir"] == str(parquet_dir)
#
# # --------------------------------------------------------
# # FULL PIPELINE：A.csv.7z → tmp/a.csv → parquet/a.parquet
# # --------------------------------------------------------
# def test_pipeline_full_flow(tmp_path):
#     raw_dir, parquet_dir, tmp_dir = setup_directories(tmp_path)
#
#     # 1. 放置 a.csv.7z 到 raw 目录
#     zfile = create_7z_with_csv(tmp_path)
#     (raw_dir / zfile.name).write_bytes(zfile.read_bytes())
#
#     # 2. 确认 7z 内容是 a.csv（防止以后谁改错 arcname）
#     with py7zr.SevenZipFile(zfile, "r") as a:
#         names = a.getnames()
#         assert names == ["a.csv"]
#
#     # 3. 跑 pipeline
#     with patch("src.data.pipeline.FTPDownloader") as mock_dl:
#         pipeline = DataPipeline()
#         pipeline.run(DATE)
#         # raw 已存在 → 不会触发下载
#         mock_dl.return_value.download.assert_not_called()
#
#     # 4. 不再强制要求 tmp/a.csv 存在（可能已被清理）
#     #    仅检查 tmp 目录存在即可（可选）
#     assert tmp_dir.exists()
#
#     # 5. 检查 parquet 是否正确生成
#     parquet_file = parquet_dir / "a.parquet"
#     assert parquet_file.exists(), f"未生成 parquet: {parquet_file}"
#
#     table = pq.read_table(parquet_file)
#     assert table.num_rows == 1
#     assert table.column("symbol").to_pylist() == ["AAPL"]
#     assert table.column("price").to_pylist() == [100]
