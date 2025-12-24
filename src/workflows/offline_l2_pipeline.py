#!filepath: src/workflows/offline_l2_pipeline.py
from __future__ import annotations

from src.pipeline.pipeline import DataPipeline
from src.utils.path import PathManager
from src.config.app_config import AppConfig
from src.observability.instrumentation import Instrumentation

from src.engines.convert_engine import ConvertEngine
from src.steps.csv_convert_step import CsvConvertStep

from src.engines.normalize_engine import NormalizeEngine
from src.steps.normalize_step import NormalizeStep

# from src.engines.symbol_split_engine import SymbolSplitEngine
# from src.steps.symbol_split_step import SymbolSplitStep
from src.steps.trade_enrich_step import TradeEnrichStep
from src.engines.trade_enrich_engine import TradeEnrichEngine

from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.steps.orderbook_rebuild_step import OrderBookRebuildStep
from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine
from src.steps.minute_trade_agg_step import MinuteTradeAggStep

from src.engines.minute_order_agg_engine import MinuteOrderAggEngine
from src.steps.minute_order_agg_step import MinuteOrderAggEngine, MinuteOrderAggStep

from src.engines.ftp_download_engine import FtpDownloadEngine
from src.steps.download_step import DownloadStep
from src.engines.trade_enrich_engine import TradeEnrichEngine

def build_offline_l2_pipeline() -> DataPipeline:
    """
    Offline Level-2 Pipeline (FINAL / FROZEN)

    Semantic Order (LAW):
        Download
        → CsvConvert              (vendor CSV → vendor parquet)
        → Normalize               (vendor → canonical Events.parquet)
        → SymbolSplit             (canonical → symbol-partitioned)
        → TradeEnrich             (canonical enrichment)
        → AggregateMinuteBar      (1m bars)
        → OrderBookRebuild        (optional, replay / research)

    After Normalize:
        - ONLY canonical schema allowed
        - Symbol MUST exist
        - ts MUST be int (microsecond)
        - vendor fields FORBIDDEN
    """

    cfg = AppConfig.load()
    pm = PathManager()
    inst = Instrumentation()

    # ----------- 非并行 Step（保留 engine）-----------
    download_step = DownloadStep(
        engine=FtpDownloadEngine(),
        inst=inst,
        secret=cfg.secret,
        remote_root=cfg.data.remote_dir,
    )

    normalize_steps = NormalizeStep(
        engine=NormalizeEngine(),
        inst=inst,
    )

    # ----------- 并行 Step（不传 engine）-----------
    extractor_steps = CsvConvertStep(inst=inst)

    trade_step = TradeEnrichStep(inst=inst, engine=TradeEnrichEngine())
    #
    min_trade_step = MinuteTradeAggStep(inst=inst, engine=MinuteTradeAggEngine())
    #
    # min_order_step = MinuteOrderAggStep(inst=inst)
    #


    steps = [
        # download_step,
        extractor_steps,
        normalize_steps,
        trade_step,
        min_trade_step,
        # min_order_step,
    ]

    return DataPipeline(
        steps=steps,
        pm=pm,
        inst=inst,
    )
# python -m src.cli run 2025-11-04
