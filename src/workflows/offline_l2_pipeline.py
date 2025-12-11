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
from src.adapters.ftp_download_adapter import FtpDownloadAdapter

# engine
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.engines.ftp_download_engine import FtpDownloadEngine

# from src.dataloader.ftp_downloader import FTPDownloader
from src.dataloader.streaming_csv_split_writer.converter import StreamingCsvSplitConverter
from src.observability.instrumentation import Instrumentation


def build_offline_l2_pipeline() -> DataPipeline:
    cfg = AppConfig.load()
    pm = PathManager()
    inst = Instrumentation()

    # engine
    trade_engine = TradeEnrichEngine()
    down_engine = FtpDownloadEngine()

    # adapter
    trade_adapter = TradeEnrichAdapter(trade_engine, pm, cfg.data.symbols)
    down_adapter = FtpDownloadAdapter(user=cfg.secret.ftp_user,
                                      host=cfg.secret.ftp_host,
                                      port=cfg.secret.ftp_port,
                                      password=cfg.secret.ftp_password,
                                      inst=inst,
                                      engine=down_engine,
                                      )

    steps = [
        DownloadStep(adapter=down_adapter, inst=inst),
        CsvToParquetStep(StreamingCsvSplitConverter()),
        SymbolSplitStep(SymbolRouterAdapter(cfg.data.symbols, pm)),
        TradeEnrichStep(trade_adapter),
        # OrderBookStep(OrderBookRebuildAdapter(pm), cfg.data.symbols),
    ]

    return DataPipeline(steps, pm, inst)
