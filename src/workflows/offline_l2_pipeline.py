#!filepath: src/workflows/offline_l2_pipeline.py
from __future__ import annotations

from src.dataloader.pipeline.pipeline import DataPipeline
from src.utils.path import PathManager
from src.config.app_config import AppConfig

# step

from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.steps.csv_to_parquet_step import CsvConvertStep
from src.dataloader.pipeline.steps.symbol_split_step import SymbolSplitStep
from src.dataloader.pipeline.steps.trade_enrich_step import TradeEnrichStep

# adapter

from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
from src.adapters.csv_convert_adapter import CsvConvertAdapter
from src.adapters.symbol_router_adapter import SymbolRouterAdapter
# from src.adapters.orderbook_rebuild_adapter import OrderBookRebuildAdapter
from src.adapters.ftp_download_adapter import FtpDownloadAdapter

# engine
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.engines.ftp_download_engine import FtpDownloadEngine
from src.engines.csv_convert_engine import CsvConvertEngine

# from src.dataloader.ftp_downloader import FTPDownloader
from src.observability.instrumentation import Instrumentation
from src.dataloader.pipeline.steps.download_step import DownloadStep


def build_offline_l2_pipeline() -> DataPipeline:
    cfg = AppConfig.load()
    pm = PathManager()
    inst = Instrumentation()

    # engine
    trade_engine = TradeEnrichEngine()
    down_engine = FtpDownloadEngine()
    convert_engine = CsvConvertEngine()

    # adapter
    trade_adapter = TradeEnrichAdapter(trade_engine, pm, cfg.data.symbols)
    down_adapter = FtpDownloadAdapter(user=cfg.secret.ftp_user,
                                      host=cfg.secret.ftp_host,
                                      port=cfg.secret.ftp_port,
                                      password=cfg.secret.ftp_password,
                                      inst=inst,
                                      engine=down_engine,
                                      )
    convert_adapter = CsvConvertAdapter(convert_engine, inst=inst)
    # trade_adapter = TradeEnrichAdapter()

    steps = [
        DownloadStep(adapter=down_adapter, inst=inst),
        CsvConvertStep(convert_adapter, inst=inst),
        SymbolSplitStep(SymbolRouterAdapter(cfg.data.symbols, pm)),
        TradeEnrichStep(trade_adapter),
        # OrderBookStep(OrderBookRebuildAdapter(pm), cfg.data.symbols),
    ]

    return DataPipeline(steps, pm, inst)
