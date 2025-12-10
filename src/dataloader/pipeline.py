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
from src.l2.event_parser import parse_events
from src.l2.orderbook.orderbook_rebuilder import OrderBookRebuilder
from src.l2.orderbook.trade_enricher import TradeEnricher
import pandas as pd

from src.observability.instrumentation import Instrumentation


class DataPipeline:
    """
    Level-2 数据处理 Pipeline：

        raw/.7z
            → tmp/csv
            → parquet
            → SH 拆分（ShConverter）
            → symbol 拆分（SymbolRouter）
            → trade enrich（TradeEnricher）
            → orderbook snapshot（OrderBookRebuilder）
            → 后续分钟聚合 / 特征工程使用

    所有「文件是否存在 → 是否跳过」的逻辑，都统一放在 Pipeline 层。
    """

    def __init__(self):
        self.cfg = AppConfig.load()
        self.downloader = FTPDownloader()
        self.decompressor = Decompressor()
        self.converter = CSVToParquetConverter()
        self.router = SymbolRouter(self.cfg.data.symbols)
        self.shc = ShConverter()

        self.path_manager = PathManager()

        self.orderbook_rebuilder = OrderBookRebuilder()

        self.trade_enricher = TradeEnricher(burst_window_ms=5)

        # ⭐ 新增：全局 instrumentation
        self.inst = Instrumentation(enabled=True)

    # -----------------------------------------------------
    def run(self, date: str):
        """
        处理某个交易日的全部 Level-2 数据。
        """
        logs.info(f"[Pipeline] ==== 开始处理 {date} ====")

        with self.inst.timer("pipeline_total"):
            # -----------------------------------
            # 目录准备
            # -----------------------------------
            with self.inst.timer("准备目录"):
                raw_dir = self.path_manager.raw_dir(date)
                parquet_date_dir = self.path_manager.parquet_dir() / date
                tmp_dir = self.path_manager.tmp_dir(date)

                FileSystem.ensure_dir(raw_dir)
                FileSystem.ensure_dir(parquet_date_dir)

            # # ==============================================================
            # # Step 1 — FTP 下载：raw/.7z 若已存在 → 跳过下载
            # # ==============================================================

            with self.inst.timer("FTP 下载"):
                seven_z_files = list(raw_dir.glob("*.7z"))

                if not seven_z_files:
                    logs.info("[Pipeline] raw/*.7z 不存在 → 进行 FTP 下载")
                    self.downloader.download(date)
                    seven_z_files = list(raw_dir.glob("*.7z"))

                    if not seven_z_files:
                        logs.warning(
                            f"[Pipeline] FTP 下载完成但未发现 .7z 文件: raw_dir={raw_dir}"
                        )

                else:
                    logs.info("[Pipeline] raw/*.7z 已存在 → 跳过下载")

            #
            # ==============================================================
            # Step 2 — 逐个 .7z 解压 → csv → parquet → SH 拆分
            # ==============================================================
            with self.inst.timer("解压+转换+SH拆分"):
                # 上交所混合文件拆分后的目标文件（存在即认为 SH 拆分已完成）
                sh_order_parquet = parquet_date_dir / "SH_order.parquet"
                sh_trade_parquet = parquet_date_dir / "SH_trade.parquet"

                for zfile in seven_z_files:
                    # 例如：SH_Order.csv.7z → stem = "SH_Order.csv" → base_stem = "SH_Order"
                    stem = zfile.stem.replace(".csv", "")
                    parquet_file = parquet_date_dir / f"{stem}.parquet"

                    # ------------------------------
                    # 2.1 SH 文件：若已成功拆分过 → 完全跳过
                    # ------------------------------
                    if "SH" in stem and sh_order_parquet.exists() and sh_trade_parquet.exists():
                        logs.info(
                            f"[Pipeline] SH 拆分结果已存在:  → 跳过 {zfile.name}"
                        )
                        continue

                    # ------------------------------
                    # 2.2 非 SH 文件：parquet 已存在 → 跳过转换
                    # ------------------------------
                    if 'SH' not in stem and parquet_file.exists():
                        logs.info(f"[Pipeline] parquet 已存在：{parquet_file} → {zfile.name}")
                        continue

                    tmp_csv = tmp_dir / f"{stem}.csv"

                    # ------ 检查parquet文件 ------
                    if not parquet_file.exists():
                        logs.info(
                            f"[Pipeline] parquet 未找到 → 需要解压 + 转换: stem={stem}, zfile={zfile.name}"
                        )

                        FileSystem.ensure_dir(tmp_dir)
                        logs.info(f"[Pipeline] 解压 {zfile} → {tmp_dir}")
                        self.decompressor.extract_7z(zfile, str(tmp_dir))
                        #
                        # ------------------------------
                        # 2.3 解压 CSV
                        # ------------------------------
                        if not tmp_csv.exists():
                            self.decompressor.extract_7z(zfile, str(tmp_dir))
                        # ------------------------------
                        # 2.4 CSV → Parquet（解析层）
                        # ------------------------------
                        self.converter.convert(tmp_csv, relative_dir=str(parquet_date_dir))
                        FileSystem.remove(tmp_csv)
                    #
                    # ------------------------------
                    # 2.5 SH 混合文件拆分（ShConverter）
                    # ------------------------------
                    # 注意：深交所文件中带 SZ，直接跳过 SH 拆分逻辑
                    if 'SZ' in zfile.name:
                        continue

                    logs.info(f"[Pipeline] 检测到上交所混合文件 → 拆分: {parquet_file}")
                    self.shc.split(parquet_file)
        #
        # ==============================================================
        # Step 3 — SymbolRouter（按 symbol 拆分）
        # ==============================================================
        with self.inst.timer("SymbolRouter"):

            self.router.route_date(date)
        #
        # ==============================================================
        # Step 4 — Trade Enrich（逐 symbol，若已存在则跳过）
        # ==============================================================
        with self.inst.timer("Trade Enrich"):
            logs.info(f"[Pipeline] === Trade Enrich 开始: {date} ===")

            for symbol in self.cfg.data.symbols:
                # 假设 SymbolRouter 输出路径为：
                # data/symbol/<symbol>/<date>/Trade.parquet
                sym_dir = PathManager.symbol_dir(symbol, date)

                trade_path = sym_dir / "Trade.parquet"
                enriched_path = sym_dir / "Trade_Enriched.parquet"

                # 4.1 enriched 已存在 → 跳过
                if enriched_path.exists():
                    # logs.info(f"[Enrich] {symbol} enriched 已存在 → 跳过")
                    continue

                # 4.2 原始 Trade 不存在 → 跳过
                if not trade_path.exists():
                    # logs.warning(f"[Enrich] {symbol} Trade.parquet 不存在 → 跳过")
                    continue

                # 4.3 读原始 trade → 事件解析（构造 ts 等）→ enrich
                logs.debug(f"[Enrich] 读取原始 Trade: {trade_path}")
                df_raw = pd.read_parquet(trade_path)
                logs.debug(f"[Enrich] 处理 {symbol} → {trade_path}")

                df_enriched = parse_events(df_raw, kind='trade')
                df_enriched.to_parquet(enriched_path, index=False)

            logs.info(f"[Pipeline] === Trade Enrich 完成 ===")

        # ==============================================================
        # Step 5 — OrderBook Snapshot 重建
        # ==============================================================
        with self.inst.timer("OrderBook Snapshot"):
            logs.info(f"[Pipeline] === OrderBook Snapshot 重建: {date} ===")

            rebuilder = OrderBookRebuilder()

            for symbol in self.cfg.data.symbols:
                # ===================== 重建 Snapshot =====================
                order_path = self.path_manager.order_dir(symbol, date)
                trade_path = self.path_manager.trade_dir(symbol, date)

                if not order_path.exists():
                    logs.warning(f'[order] symbol={symbol} not found {order_path}')
                    continue
                if not trade_path.exists():
                    logs.warning(f'[trade] symbol={symbol}  not found {trade_path}')
                    continue

                # ⭐ 任意层级都可再加 timer
                # with self.inst.timer(f"OrderBook_{symbol}"):
                rebuilder.build(symbol, date, write=True)

        # -------------------------------------------------
        # Pipeline 完成，生成 Timeline 报告
        # -------------------------------------------------
        self.inst.generate_timeline_report(date)
