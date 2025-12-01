#!filepath: src/dataloader/pipeline.py


from src.dataloader.ftp_downloader import FTPDownloader
from src.dataloader.decompressor import Decompressor
from src.dataloader.csv_converter import CSVToParquetConverter
from src.dataloader.sh_converter import ShConverter
from src.dataloader.symbol_router import SymbolRouter

from src.utils.filesystem import FileSystem
from src.utils.path import PathManager
from src.config.app_config import AppConfig
from src import logs


class DataPipeline:
    """
    Level2 Pipeline:
        raw/.7z → tmp/csv → parquet → SH拆分 → symbol → minute → feature
    """

    def __init__(self):
        self.cfg = AppConfig.load()
        self.downloader = FTPDownloader()
        self.decompressor = Decompressor()
        self.converter = CSVToParquetConverter()
        self.router = SymbolRouter(self.cfg.data.symbols)
        self.shc = ShConverter()

        self.path_manager = PathManager()



    # -----------------------------------------------------
    def run(self, date: str):
        logs.info(f"[Pipeline] ==== 开始处理 {date} ====")

        raw_dir = self.path_manager.raw_dir() / date
        parquet_dir = self.path_manager.parquet_dir() / date
        tmp_dir = self.path_manager.temp_dir() / "decompress" / date
        #
        FileSystem.ensure_dir(raw_dir)
        FileSystem.ensure_dir(parquet_dir)

        # -----------------------------------------------------
        # Step 1 — raw/.7z 若存在 → 跳过下载
        # -----------------------------------------------------
        seven_z_files = list(raw_dir.glob("*.7z"))

        if not seven_z_files:
            logs.info("[Pipeline] raw 不存在 → 进行 FTP 下载")
            self.downloader.download(date)
            seven_z_files = list(raw_dir.glob("*.7z"))
        else:
            logs.info("[Pipeline] raw 已存在 → 跳过下载")

        # -----------------------------------------------------
        # Step 2 —  逐个 .7z 解压 → tmp csv → parquet → SH 拆分
        # -----------------------------------------------------
        order_file = parquet_dir / "SH_order.parquet"
        trade_file = parquet_dir / "SH_trade.parquet"

        for zfile in seven_z_files:
            stem = zfile.stem.replace(".csv", "")
            parquet_file = parquet_dir / f"{stem}.parquet"

            # ------ 1.  SH 文件 + 已产生拆分文件 → 完全跳过 ------
            if 'SH' in stem and order_file.exists() and trade_file.exists():

                msg = f"[Pipeline] SH 已存在：{order_file}, {trade_file} → 跳过 {stem}"
                logs.info(msg)
                continue

            # ------ 2. B. 非 SH：若 parquet 已存在 → 跳过转换------
            if 'SH' not in stem and parquet_file.exists():
                logs.info(f"[Pipeline] parquet 已存在：{parquet_file} → 跳过")
                continue

            # ======================================================
            #                解压 + csv 转换 parquet
            # ======================================================

            # ------ 3. 检查parquet文件 ------
            if not parquet_file.exists():
                logs.info(f"[Pipeline] parquet 未找到 → 需要 {stem} 解压 + 转换")

                tmp_csv = tmp_dir / f"{stem}.csv"

                # ------ 4. 检查csv文件 ------

                if not tmp_csv.exists():
                    FileSystem.ensure_dir(tmp_dir)
                    self.decompressor.extract_7z(zfile, str(tmp_dir))
                # ------ 5. csv->parquet ------
                self.converter.convert(tmp_csv, relative_dir=str(parquet_dir))
                FileSystem.remove(tmp_csv)

            # -----------------------------------------------------
            # ★ 6 — 对 parquet 进行 SH 拆分（ShConverter）
            # # # -----------------------------------------------------
            logs.info(f"[Pipeline] 检测到上交所混合文件 → 拆分: {parquet_file}")
            self.shc.split(parquet_file)
            FileSystem.remove(parquet_file)

        # -----------------------------------------------------
        # ★ Step 7 — SymbolRouter（按 symbol 拆分）
        # -----------------------------------------------------
        self.router.route_date(date)

        logs.info(f"[Pipeline] ==== 完成处理 {date} ====")
