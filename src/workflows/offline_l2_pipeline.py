#!filepath: src/workflows/offline_l2_pipeline.py
from __future__ import annotations

from src.config.app_config import AppConfig
from src.utils.path import PathManager

from src.dataloader.ftp_downloader import FTPDownloader
from src.dataloader.streaming_csv_split_writer.converter import StreamingCsvSplitConverter
from src.dataloader.symbol_router import SymbolRouter
from src.l2.orderbook.orderbook_rebuilder import OrderBookRebuilder

from src.adapters.l2_raw_to_parquet_adapter import L2RawToParquetAdapter
from src.adapters.symbol_split_adapter import SymbolSplitAdapter
from src.adapters.parquet_trade_event_source import ParquetTradeEventSource
from src.adapters.enriched_trade_sink import EnrichedTradeSink
from src.adapters.orderbook_adapter import OrderBookAdapter

from src.engines.trade_enrich_engine_impl import TradeEnrichEngineImpl

from src.dataloader.pipeline.pipeline import DataPipeline
from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.steps.csv_to_parquet_step import CsvToParquetStep
from src.dataloader.pipeline.steps.symbol_split_step import SymbolSplitStep
from src.dataloader.pipeline.steps.trade_enrich_step import TradeEnrichStep
from src.dataloader.pipeline.steps.orderbook_rebuild_step import OrderBookStep


def build_offline_l2_pipeline() -> DataPipeline:
    """
    工厂函数：根据 AppConfig / PathManager 构造完整 Offline L2 Pipeline
    """
    cfg = AppConfig.load()
    pm = PathManager()

    symbols = cfg.data.symbols  # 假设是 [600000, 300750, ...]

    # 核心依赖
    downloader = FTPDownloader()
    converter = StreamingCsvSplitConverter()
    router = SymbolRouter(symbols, pm)
    rebuilder = OrderBookRebuilder()

    # Adapters
    raw_to_parquet = L2RawToParquetAdapter(downloader, converter)
    symbol_splitter = SymbolSplitAdapter(router)
    trade_source = ParquetTradeEventSource(pm, symbols)
    enriched_sink = EnrichedTradeSink(pm)
    orderbook_adapter = OrderBookAdapter(pm)

    # Engines
    trade_engine = TradeEnrichEngineImpl(burst_window_ms=5)

    # Steps
    steps = [
        DownloadStep(downloader),              # 也可以用 raw_to_parquet 内部下载，这里保留独立 Step
        CsvToParquetStep(raw_to_parquet),
        # SymbolSplitStep(symbol_splitter),
        TradeEnrichStep(trade_source, pm, enriched_sink),
        OrderBookStep(orderbook_adapter, symbols),
    ]

    pipeline = DataPipeline(steps=steps, path_manager=pm)
    return pipeline
