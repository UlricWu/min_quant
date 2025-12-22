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

from src.engines.symbol_split_engine import SymbolSplitEngine
from src.steps.symbol_split_step import SymbolSplitStep
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

    download_engine = FtpDownloadEngine()
    download_step = DownloadStep(engine=download_engine, inst=inst, secret=cfg.secret, remote_root=cfg.data.remote_dir)

    extractor_engine = ConvertEngine()
    extractor_steps = CsvConvertStep(engine=extractor_engine, inst=inst)

    normalize_engine = NormalizeEngine()
    normalize_steps = NormalizeStep(engine=normalize_engine, inst=inst)
    symbol_split_engine = SymbolSplitEngine()
    symbol_split_steps = SymbolSplitStep(engine=symbol_split_engine, inst=inst)

    trade_engine = TradeEnrichEngine()
    trade_step = TradeEnrichStep(engine=trade_engine, inst=inst)

    order_engine = OrderBookRebuildEngine()
    order_step = OrderBookRebuildStep(engine=order_engine, inst=inst)

    min_trade_engine = MinuteTradeAggEngine()
    min_trade_step = MinuteTradeAggStep(engine=min_trade_engine, inst=inst)

    min_order_engine = MinuteOrderAggEngine()
    min_order_step = MinuteOrderAggStep(engine=min_order_engine, inst=inst)

    steps = [download_step, extractor_steps, normalize_steps, symbol_split_steps, trade_step, order_step,
             min_trade_step, min_order_step]

    return DataPipeline(
        steps=steps,
        pm=pm,
        inst=inst,
    )
# python -m src.cli run 2025-11-04
