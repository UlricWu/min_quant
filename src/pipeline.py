#!filepath: src/dataloader/pipeline.py

from __future__ import annotations

from src.dataloader.ftp_downloader import FTPDownloader
from src.dataloader.streaming_csv_split_writer.converter import StreamingCsvSplitConverter
from src.dataloader.symbol_router import SymbolRouter
from src.l2.offline.offline_batch_runner import OfflineBatchRunner

from src.utils.filesystem import FileSystem
from src.utils.path import PathManager
from src.config.app_config import AppConfig
from src import logs

from src.observability.instrumentation import Instrumentation


class DataPipeline:
    """
    Level-2 数据处理 Pipeline（最新 Streaming 版本）：

        Step1. FTP 下载
        Step2. 7z → CSV streaming → parquet（SH/SZ 分类）
        Step3. SymbolRouter（按 symbol 拆分）
        Step4. OfflineBatchRunner（逐 symbol / date 生成逐笔特征）

    所有 skip/exists 判断都在 Pipeline，不在组件内部。
    """

    def __init__(self) -> None:
        self.cfg = AppConfig.load()
        self.downloader = FTPDownloader()

        self.path_manager = PathManager()
        self.inst = Instrumentation()

        # CSV → SH_Order / SH_Trade / SZ_*.parquet（全 streaming）
        self.csv_converter = StreamingCsvSplitConverter()

        ## SymbolRouter（路径策略由 Pipeline 注入）
        self.router = SymbolRouter(
            symbols=self.cfg.data.symbols,
            path_manager=self.path_manager,
        )

        # Offline 逐笔特征生成（Trade_Enriched）
        self.offline_runner = OfflineBatchRunner(
            path_manager=self.path_manager,
            symbols=self.cfg.data.symbols,
        )

    # ---------------------------------------------------
    # 判定 file_type（交给 converter）
    # ---------------------------------------------------
    def _detect_file_type(self, filename: str) -> str:
        lower = filename.lower()

        if lower.startswith("sh_stock_ordertrade"):
            return "SH_MIXED"

        if lower.startswith("sh_order"):
            return "SH_ORDER"
        if lower.startswith("sh_trade"):
            return "SH_TRADE"

        if lower.startswith("sz_order"):
            return "SZ_ORDER"
        if lower.startswith("sz_trade"):
            return "SZ_TRADE"

        raise RuntimeError(f"无法识别文件类型：{filename}")

    # ---------------------------------------------------
    def run(self, date: str) -> None:
        logs.info(f"[Pipeline] ==== 开始处理 {date} ====")

        with self.inst.timer("pipeline_total"):

            # ==============================================================
            # Step 0 — 目录准备
            # ==============================================================
            with self.inst.timer("准备目录"):
                raw_dir = self.path_manager.raw_dir(date)
                parquet_root = self.path_manager.parquet_dir()
                parquet_date_dir = parquet_root / date

                FileSystem.ensure_dir(raw_dir)
                FileSystem.ensure_dir(parquet_date_dir)

            # ==============================================================
            # Step 1 — FTP 下载
            # ==============================================================
            with self.inst.timer("FTP 下载"):
                seven_z_files = list(raw_dir.glob("*.7z"))

                if not seven_z_files:
                    logs.info("[Pipeline] raw/*.7z 不存在 → 进行 FTP 下载")
                    self.downloader.download(date)
                    seven_z_files = list(raw_dir.glob("*.7z"))

                    if not seven_z_files:
                        logs.warning(f"[Pipeline] FTP 下载完成但 raw 下仍无 .7z 文件")
                else:
                    logs.info("[Pipeline] raw/*.7z 已存在 → 跳过下载")

            # ==============================================================
            # Step 2 — CSV streaming 转 parquet（SH/SZ 自动分流）
            # ==============================================================
            with self.inst.timer("解压+转换+SH拆分"):

                parquet_date_dir = self.path_manager.parquet_dir() / date
                sh_order_parquet = parquet_date_dir / "SH_Order.parquet"
                sh_trade_parquet = parquet_date_dir / "SH_Trade.parquet"

                for zfile in seven_z_files:
                    stem = zfile.stem.replace(".csv", "")
                    file_type = self._detect_file_type(zfile.name)

                    # skip 逻辑：以业务层为准
                    if file_type == "SH_MIXED":
                        if sh_order_parquet.exists() and sh_trade_parquet.exists():
                            logs.info(
                                f"[Pipeline] SH_Order / SH_Trade 已存在 → 跳过 {zfile.name}"
                            )
                            continue
                    else:
                        parquet_file = parquet_date_dir / f"{stem}.parquet"
                        if parquet_file.exists():
                            logs.info(
                                f"[Pipeline] {parquet_file} 已存在 → 跳过 {zfile.name}"
                            )
                            continue

                    logs.info(f"[Pipeline] 开始处理 L2 CSV → parquet: {zfile}")
                    self.csv_converter.convert(zfile, parquet_date_dir, file_type)

            # ==============================================================
            # Step 3 — SymbolRouter（按 symbol 拆分）
            # ==============================================================
            with self.inst.timer("SymbolRouter"):
                self.router.route_date(date)
                logs.info(f"[SymbolRouter] ==== 完成 Symbol router date={date} ====")

            # ==============================================================
            # Step 4 — OfflineBatchRunner（生成逐笔特征）
            # ==============================================================
            with self.inst.timer("TradeFeatures"):
                logs.info(f"[OfflineBatchRunner] ==== 生成逐笔特征 date={date} ====")
                # 若 OfflineBatchRunner 内部使用 AppConfig.data.symbols，
                # 这里无需再传 symbols；如果你想只跑部分标的，可以传入 symbols 参数。
                self.offline_runner.run_date(date)
                logs.info(f"[OfflineBatchRunner] ==== 完成逐笔特征 date={date} ====")

            logs.info(f"[Pipeline] ==== 完成处理 {date} ====")

            # 输出 timeline 报表
            self.inst.generate_timeline_report(date)
