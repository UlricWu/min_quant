#!filepath: src/workflows/offline_l2_pipeline.py
from __future__ import annotations

from src.dataloader.pipeline.pipeline import DataPipeline
from src.utils.path import PathManager

from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.steps.csv_to_parquet_step import CsvToParquetStep
from src.dataloader.pipeline.steps.symbol_split_step import SymbolSplitStep
from src.dataloader.pipeline.steps.trade_enrich_step import TradeEnrichStep
from src.dataloader.pipeline.steps.orderbook_step import OrderBookStep

from src.dataloader.streaming_csv_split_writer.converter import StreamingCsvSplitConverter
from src.dataloader.symbol_router import SymbolRouter
# from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
# from src.adapters.orderbook_rebuild_adapter import OrderBookRebuildAdapter

from src.dataloader.ftp_downloader import FTPDownloader
from src.config.app_config import AppConfig


def build_offline_l2_pipeline():

    cfg = AppConfig.load()
    pm = PathManager()

    steps = [
        DownloadStep(FTPDownloader()),
        CsvToParquetStep(StreamingCsvSplitConverter()),
        SymbolSplitStep(SymbolRouter(cfg.data.symbols, pm)),
        # TradeEnrichStep(TradeEnrichAdapter(pm, cfg.data.symbols)),
        # OrderBookStep(OrderBookRebuildAdapter(pm), cfg.data.symbols),
    ]

    return DataPipeline(steps, pm)
