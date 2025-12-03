# #!filepath: tests/dataloader_test/test_pipeline_smart_skip.py
# from pathlib import Path
#
# import pytest
#
# from src.dataloader.pipeline import DataPipeline
#
#
# # ================================================================
# # Dummy Components (Mocks)
# # ================================================================
# class DummyDownloader:
#     def __init__(self, base: Path):
#         self.base = base
#         self.called = False
#
#     def download(self, date: str):
#         """
#         模拟 FTP 下载：在 raw/date 下创建一个 SH_test.7z
#         """
#         self.called = True
#         raw_dir = self.base / "raw" / date
#         raw_dir.mkdir(parents=True, exist_ok=True)
#         (raw_dir / "SH_test.7z").write_text("dummy")
#
#
# class DummyDecompressor:
#     def __init__(self):
#         self.called = False
#
#     def extract_7z(self, zfile: Path, out_dir: str):
#         self.called = True
#         p = Path(out_dir)
#         p.mkdir(parents=True, exist_ok=True)
#         (p / f"{zfile.stem}.csv").write_text("col\n1")
#
#
# class DummyConverter:
#     def __init__(self):
#         self.called = False
#
#     def convert(self, csv_file: Path, relative_dir: str):
#         self.called = True
#         p = Path(relative_dir)
#         p.mkdir(parents=True, exist_ok=True)
#         (p / f"{csv_file.stem}.parquet").write_text("parquet")
#
#
# class DummyShConverter:
#     def __init__(self):
#         self.called = False
#
#     def split(self, parquet_file: Path):
#         self.called = True
#         out_dir = parquet_file.parent
#         # 模拟生成 SH 拆分后的两个文件
#         (out_dir / "SH_order.parquet").write_text("order")
#         (out_dir / "SH_trade.parquet").write_text("trade")
#
#
# class DummySymbolRouter:
#     def __init__(self, *_, **__):
#         self.called = False
#
#     def route_date(self, date: str):
#         self.called = True
#
#
# # ================================================================
# # Mock PathManager，让 pipeline 写入 tmp_path
# # ================================================================
# class PM:
#     def __init__(self, base: Path):
#         self.base = base
#
#     def raw_dir(self) -> Path:
#         return self.base / "raw"
#
#     def parquet_dir(self) -> Path:
#         return self.base / "parquet"
#
#     def temp_dir(self) -> Path:
#         return self.base / "tmp"
#
#
# # ================================================================
# # Fixture: monkeypatch 环境
# # ================================================================
# @pytest.fixture
# def setup_env(tmp_path: Path, monkeypatch):
#     base = tmp_path
#
#     # PathManager → PM(tmp_path)
#     monkeypatch.setattr("src.dataloader.pipeline.PathManager", lambda: PM(base))
#
#     # 各组件注入 Dummy
#     monkeypatch.setattr(
#         "src.dataloader.pipeline.FTPDownloader",
#         lambda: DummyDownloader(base),
#     )
#     monkeypatch.setattr(
#         "src.dataloader.pipeline.Decompressor",
#         lambda: DummyDecompressor(),
#     )
#     monkeypatch.setattr(
#         "src.dataloader.pipeline.CSVToParquetConverter",
#         lambda: DummyConverter(),
#     )
#     monkeypatch.setattr(
#         "src.dataloader.pipeline.ShConverter",
#         lambda: DummyShConverter(),
#     )
#     monkeypatch.setattr(
#         "src.dataloader.pipeline.SymbolRouter",
#         lambda symbols: DummySymbolRouter(),
#     )
#
#     return base
#
#
# # ================================================================
# # 自动推导期望行为的 smart-skip 测试
# # ================================================================
# @pytest.mark.parametrize(
#     "has_7z, has_tmp_csv, has_parquet, has_sh_order, has_sh_trade",
#     [
#         # 1. 全都没有：需要 下载 + 解压 + 转换 + 拆分
#         (False, False, False, False, False),
#
#         # 2. 只有 .7z：需要 解压 + 转换 + 拆分
#         (True, False, False, False, False),
#
#         # 3. 已有 tmp.csv：跳过解压，仅 转换 + 拆分
#         (True, True, False, False, False),
#
#         # 4. 已有 parquet + SH_order + SH_trade：整个 SH 文件直接跳过
#         (True, True, True, True, True),
#
#         # 5. 已有 parquet，但 SH_order/SH_trade 不全：跳过 convert，只做拆分
#         (True, True, True, False, False),
#     ],
# )
# def test_pipeline_smart_skip_auto(
#     setup_env: Path,
#     has_7z: bool,
#     has_tmp_csv: bool,
#     has_parquet: bool,
#     has_sh_order: bool,
#     has_sh_trade: bool,
# ):
#     base = setup_env
#     date = "20250101"
#
#     raw_dir = base / "raw" / date
#     parquet_dir = base / "parquet" / date
#     tmp_dir = base / "tmp" / "decompress" / date
#
#     raw_dir.mkdir(parents=True, exist_ok=True)
#     parquet_dir.mkdir(parents=True, exist_ok=True)
#     tmp_dir.mkdir(parents=True, exist_ok=True)
#
#     # ----------------------------
#     # 1. 初始化文件状态
#     # ----------------------------
#     zfile = raw_dir / "SH_test.7z"
#     if has_7z:
#         zfile.write_text("dummy")
#
#     tmp_csv = tmp_dir / "SH_test.csv"
#     if has_tmp_csv:
#         tmp_csv.write_text("col\n1")
#
#     parquet_file = parquet_dir / "SH_test.parquet"
#     if has_parquet:
#         parquet_file.write_text("parquet")
#
#     order_file = parquet_dir / "SH_order.parquet"
#     trade_file = parquet_dir / "SH_trade.parquet"
#     if has_sh_order:
#         order_file.write_text("order")
#     if has_sh_trade:
#         trade_file.write_text("trade")
#
#     # ----------------------------
#     # 2. 运行 pipeline
#     # ----------------------------
#     pipeline = DataPipeline()
#     pipeline.run(date)
#
#     # 取出 Dummy 实例（DataPipeline 在 __init__ 中注入）
#     downloader = pipeline.downloader
#     decompressor = pipeline.decompressor
#     converter = pipeline.converter
#     shc = pipeline.shc
#     router = pipeline.router
#
#     # ----------------------------
#     # 3. 按 pipeline 真实逻辑“自动推导期望值”
#     # ----------------------------
#     #
#     # 注意：这里的推导逻辑 == pipeline.run() 里的决策逻辑，
#     # 这样以后 pipeline 改逻辑，只要同步调这里，测试不会因为
#     # “手工 expected 写错”而挂。
#     #
#     # --- FTPDownloader ---
#     # 如果一开始 raw_dir 下没有 .7z，则会触发 download(date)
#     expected_download = not has_7z
#
#     # --- Decompressor.extract_7z ---
#     # pipeline 中的逻辑：
#     #   if not parquet_file.exists():
#     #       if not tmp_csv.exists():
#     #           extract_7z
#     #
#     expected_extract = (not has_parquet) and (not has_tmp_csv)
#
#     # --- Converter.convert ---
#     # pipeline 中的逻辑：
#     #   if is_sh and sh_exist:  # (SH_order & SH_trade 都存在)
#     #       continue  # 完全跳过本 zfile
#     #   ...
#     #   if not parquet_file.exists():
#     #       ...  # 这里才会 convert
#     #
#     # 我们这里只有 SH_test.7z，所以 is_sh 永远 True
#     sh_exist = has_sh_order and has_sh_trade
#     expected_convert = (not sh_exist) and (not has_parquet)
#
#     # --- ShConverter.split ---
#     # pipeline 中的逻辑：
#     #   if is_sh and sh_exist:
#     #       continue
#     #   ...
#     #   self.shc.split(parquet_file)
#     #
#     # 只要不是 "(SH 文件且已经有 SH_order+SH_trade)" 这个跳过条件，
#     # 就一定会 split。
#     expected_split = not sh_exist
#
#     # SymbolRouter 总是在最后 route_date(date)
#     expected_router = True
#
#     # ----------------------------
#     # 4. 断言：实际调用 vs. 自动推导期望
#     # ----------------------------
#     assert downloader.called == expected_download
#     assert decompressor.called == expected_extract
#     assert converter.called == expected_convert
#     assert shc.called == expected_split
#     assert router.called == expected_router
#
#     # ----------------------------
#     # 5. 校验文件结果（根据 expected 行为推导）
#     # ----------------------------
#     # 拆分执行时，必须有 SH_order / SH_trade
#     if expected_split:
#         assert order_file.exists()
#         assert trade_file.exists()
#         # 拆分执行后，原 parquet 会被删除
#         assert not parquet_file.exists()
#     else:
#         # 如果一开始就有 SH_order/SH_trade，应该保留
#         if has_sh_order:
#             assert order_file.exists()
#         if has_sh_trade:
#             assert trade_file.exists()
#         # 如果本来就有 parquet 且被 skip，则仍然存在
#         if has_parquet:
#             assert parquet_file.exists()
