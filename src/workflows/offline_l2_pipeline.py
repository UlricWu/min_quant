#!filepath: src/workflows/offline_l2_pipeline.py
from __future__ import annotations

from src.dataloader.pipeline.pipeline import DataPipeline
from src.utils.path import PathManager
from src.config.app_config import AppConfig

# step

from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.steps.csv_to_parquet_step import CsvToParquetStep
from src.dataloader.pipeline.steps.symbol_split_step import SymbolSplitStep
from src.dataloader.pipeline.steps.trade_enrich_step import TradeEnrichStep
from src.dataloader.pipeline.steps.orderbook_step import OrderBookStep

# adapter

from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
from src.adapters.symbol_router_adapter import SymbolRouterAdapter
# from src.adapters.orderbook_rebuild_adapter import OrderBookRebuildAdapter



# engine
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.dataloader.ftp_downloader import FTPDownloader
from src.dataloader.streaming_csv_split_writer.converter import StreamingCsvSplitConverter
from src.observability.instrumentation import Instrumentation


def build_offline_l2_pipeline() -> DataPipeline:
    cfg = AppConfig.load()
    pm = PathManager()

    trade_engine = TradeEnrichEngine()
    trade_adapter = TradeEnrichAdapter(trade_engine, pm, cfg.data.symbols)
    inst = Instrumentation()

    steps = [
        DownloadStep(FTPDownloader()),
        CsvToParquetStep(StreamingCsvSplitConverter()),
        SymbolSplitStep(SymbolRouterAdapter(cfg.data.symbols, pm)),
        TradeEnrichStep(trade_adapter),
        # OrderBookStep(OrderBookRebuildAdapter(pm), cfg.data.symbols),
    ]

    return DataPipeline(steps, pm, inst)
