#!filepath: src/workflows/offline_l2_pipeline.py
from __future__ import annotations

from src.dataloader.pipeline.pipeline import DataPipeline
from src.utils.path import PathManager
from src.config.app_config import AppConfig
from src.observability.instrumentation import Instrumentation

# =========================
# Pipeline Steps
# =========================
from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.steps.csv_to_parquet_step import CsvConvertStep
from src.dataloader.pipeline.steps.normalize_step import NormalizeStep
from src.dataloader.pipeline.steps.symbol_split_step import SymbolSplitStep
from src.dataloader.pipeline.steps.trade_enrich_step import TradeEnrichStep
from src.dataloader.pipeline.steps.aggregate_minute_bar_step import AggregateMinuteBarStep
from src.dataloader.pipeline.steps.orderbook_step import OrderBookRebuildStep

# =========================
# Adapters
# =========================
from src.adapters.ftp_download_adapter import FtpDownloadAdapter
from src.adapters.convert_adapter import ConvertAdapter, SplitConvertAdapter
from src.adapters.normalize_adapter import NormalizeAdapter
from src.adapters.symbol_router_adapter import SymbolRouterAdapter
from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
from src.adapters.aggregate_minute_bar_adapter import AggregateMinuteBarAdapter
from src.adapters.orderbook_rebuild_adapter import OrderBookRebuildAdapter

# =========================
# Engines
# =========================
from src.engines.ftp_download_engine import FtpDownloadEngine
from src.engines.extractor_engine import ExtractorEngine
from src.engines.event_splitter_engine import TickTypeSplitterEngine
from src.engines.writer_engine import StreamingWriterEngine
from src.engines.normalize_engine import NormalizeEngine
from src.engines.symbol_router_engine import SymbolRouterEngine
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.engines.aggregate_minute_bar_engine import AggregateMinuteBarEngine
from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine


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

    # ============================================================
    # Engine Layer
    # ============================================================
    ftp_engine = FtpDownloadEngine()

    extractor = ExtractorEngine()
    splitter = TickTypeSplitterEngine()
    writer = StreamingWriterEngine()

    normalize_engine = NormalizeEngine()
    symbol_router_engine = SymbolRouterEngine()
    trade_enrich_engine = TradeEnrichEngine()
    minute_bar_engine = AggregateMinuteBarEngine()
    orderbook_engine = OrderBookRebuildEngine()

    # ============================================================
    # Adapter Layer
    # ============================================================

    # ---- Download ----
    ftp_adapter = FtpDownloadAdapter(
        engine=ftp_engine,
        inst=inst,
        secret=cfg.secret,
        backend=cfg.pipeline.ftp_backend,
        remote_root=cfg.data.remote_dir
    )

    # ---- CSV → Parquet (vendor schema) ----
    convert_adapter = ConvertAdapter(
        extractor=extractor,
        writer=writer,
    )

    split_convert_adapter = SplitConvertAdapter(
        extractor=extractor,
        splitter=splitter,
        writer=writer,
    )

    # ---- Normalize (vendor → canonical) ----
    normalize_adapter = NormalizeAdapter(
        engine=normalize_engine,
        symbols=cfg.data.symbols,
        inst=inst,
    )

    # ---- SymbolSplit (canonical → partition) ----
    symbol_router_adapter = SymbolRouterAdapter(
        engine=symbol_router_engine,
        inst=inst,
    )

    # ---- Trade Enrich (canonical only) ----
    trade_enrich_adapter = TradeEnrichAdapter(
        engine=trade_enrich_engine,
        symbols=cfg.data.symbols,
        inst=inst,
    )

    # ---- Minute Bar ----
    # minute_bar_adapter = AggregateMinuteBarAdapter(
    #     engine=minute_bar_engine,
    #     out_root=pm.bar_1m_root,
    # )

    # ---- OrderBook (optional) ----
    orderbook_adapter = OrderBookRebuildAdapter(
        engine=orderbook_engine,
        symbols=cfg.data.symbols,
        inst=inst,
    )

    # ============================================================
    # Step Layer (ORDER IS LAW)
    # ============================================================

    steps = [
        # 1️⃣ Raw download
        DownloadStep(ftp_adapter, inst=inst, engine=ftp_engine),

        # 2️⃣ CSV → vendor parquet
        CsvConvertStep(
            sh_adapter=split_convert_adapter,
            sz_adapter=convert_adapter,
            inst=inst,
        ),

        # 3️⃣ Normalize (🔥 semantic freeze 🔥)
        NormalizeStep(
            adapter=normalize_adapter,
            inst=inst,
        ),

        # 4️⃣ Canonical → symbol partition
        # SymbolSplitStep(
        #     adapter=symbol_router_adapter,
        #     inst=inst,
        #     skip_if_exists=True,
        # ),

        # 5️⃣ Trade enrich (canonical only)
        # TradeEnrichStep(
        #     adapter=trade_enrich_adapter,
        #     inst=inst,
        # ),

        # # 6️⃣ 1-minute bar aggregation
        # AggregateMinuteBarStep(
        #     adapter=minute_bar_adapter,
        #     skip_if_exists=True,
        # ),

        # 7️⃣ OrderBook rebuild (research / replay)
        # OrderBookRebuildStep(
        #     adapter=orderbook_adapter,
        #     inst=inst,
        # ),
    ]

    return DataPipeline(
        steps=steps,
        pm=pm,
        inst=inst,
    )
