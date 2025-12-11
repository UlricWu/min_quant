#!filepath: src/adapters/l2_raw_to_parquet_adapter.py
from __future__ import annotations
from pathlib import Path

from src.dataloader.ftp_downloader import FTPDownloader
from src.dataloader.streaming_csv_split_writer.converter import StreamingCsvSplitConverter
from src.utils.filesystem import FileSystem
from src import logs


class L2RawToParquetAdapter:
    """
    负责：
        raw/<date>/*.7z   →   parquet/<date>/SH_*.parquet / SZ_*.parquet

    只做：
        - 是否需要下载
        - 调用 streaming converter
        - 控制跳过逻辑

    不做：
        - symbol 拆分
        - enrich
        - orderbook
    """

    def __init__(self, downloader: FTPDownloader, converter: StreamingCsvSplitConverter):
        self.downloader = downloader
        self.converter = converter

    def ensure_parquet_for_date(self, date: str, raw_dir: Path, parquet_dir: Path) -> None:
        FileSystem.ensure_dir(raw_dir)
        FileSystem.ensure_dir(parquet_dir)

        seven_z_files = list(raw_dir.glob("*.7z"))

        if not seven_z_files:
            logs.info(f"[L2RawToParquet] {raw_dir} 下无 .7z → 开始下载")
            self.downloader.download(date)
            seven_z_files = list(raw_dir.glob("*.7z"))
        else:
            logs.info(f"[L2RawToParquet] {raw_dir} 已有 .7z → 跳过下载")

        if not seven_z_files:
            logs.warning(f"[L2RawToParquet] 下载完成但仍没有 .7z 文件: {raw_dir}")
            return

        sh_order_parquet = parquet_dir / "SH_Order.parquet"
        sh_trade_parquet = parquet_dir / "SH_Trade.parquet"

        for zfile in seven_z_files:
            stem = zfile.stem.replace(".csv", "")
            file_type = self._detect_file_type(zfile.name)

            if file_type == "SH_MIXED":
                # SH_Stock_OrderTrade → SH_Order / SH_Trade
                if sh_order_parquet.exists() and sh_trade_parquet.exists():
                    logs.info(f"[L2RawToParquet] {zfile.name} 目标已存在 → 跳过")
                    continue
            else:
                parquet_file = parquet_dir / f"{stem}.parquet"
                if parquet_file.exists():
                    logs.info(f"[L2RawToParquet] {parquet_file} 已存在 → 跳过 {zfile.name}")
                    continue

            logs.info(f"[L2RawToParquet] 开始转换: {zfile.name}")
            self.converter.convert(zfile, parquet_dir, file_type)

    @staticmethod
    def _detect_file_type(filename: str) -> str:
        low = filename.lower()

        if low.startswith("sh_stock_ordertrade"):
            return "SH_MIXED"
        if low.startswith("sh_order"):
            return "SH_ORDER"
        if low.startswith("sh_trade"):
            return "SH_TRADE"
        if low.startswith("sz_order"):
            return "SZ_ORDER"
        if low.startswith("sz_trade"):
            return "SZ_TRADE"

        raise RuntimeError(f"[L2RawToParquet] 无法识别文件类型: {filename}")
