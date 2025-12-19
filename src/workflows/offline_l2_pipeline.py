#!filepath: src/workflows/offline_l2_pipeline.py
from __future__ import annotations

from src.adapters import normalize_adapter
from src.dataloader.pipeline.pipeline import DataPipeline
from src.utils.path import PathManager
from src.config.app_config import AppConfig

# steps
from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.steps.csv_to_parquet_step import CsvConvertStep
from src.dataloader.pipeline.steps.symbol_split_step import SymbolSplitStep
from src.dataloader.pipeline.steps.normalize_step import NormalizeStep
from src.dataloader.pipeline.steps.trade_enrich_step import TradeEnrichStep
from src.dataloader.pipeline.steps.orderbook_step import OrderBookRebuildStep

# adapter
from src.adapters.convert_adapter import ConvertAdapter, SplitConvertAdapter
from src.adapters.ftp_download_adapter import FtpDownloadAdapter
from src.adapters.trade_enrich_adapter import TradeEnrichAdapter
from src.adapters.normalize_adapter import NormalizeAdapter
from src.adapters.symbol_router_adapter import SymbolRouterAdapter
from src.adapters.orderbook_rebuild_adapter import OrderBookRebuildAdapter

# engines
from src.engines.symbol_router_engine import SymbolRouterEngine
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.engines.ftp_download_engine import FtpDownloadEngine
from src.observability.instrumentation import Instrumentation
from src.engines.WriterEngine import StreamingWriterEngine
from src.engines.extractor_engine import ExtractorEngine
from src.engines.event_splitter_engine import TickTypeSplitterEngine
from src.engines.orderbook_rebuild_engine import OrderBookRebuildEngine
from src.engines.normalize_engine import NormalizeEngine

def build_offline_l2_pipeline() -> DataPipeline:
    """
    Offline L2 处理（Level-2 → parquet → symbol-split → enrich）
    三层架构：
        Workflow     = 本文件
        Pipeline     = DataPipeline
        Step         = DownloadStep / CsvConvertStep / SymbolSplitStep / TradeEnrichStep
        Adapter      = CsvConvertAdapter / ...
        Engine       = CsvConvertEngine / ...
    """

    cfg = AppConfig.load()
    pm = PathManager()
    inst = Instrumentation()

    # ----------- Engine Layer -----------
    down_engine = FtpDownloadEngine()
    # trade_engine = TradeEnrichEngine()

    # ----------- Adapter Layer -----------
    down_adapter = FtpDownloadAdapter(engine=down_engine, inst=inst, secret=cfg.secret, backend=cfg.pipeline.ftp_backend)

    tick = TickTypeSplitterEngine()
    writer = StreamingWriterEngine()

    convert_adapter = ConvertAdapter(
        extractor=ExtractorEngine(),
        writer=writer,

    )

    split_adapter = SplitConvertAdapter(extractor=ExtractorEngine(),
                                        writer=writer,
                                        splitter=tick
                                        )

    csv_convert_step = CsvConvertStep(
        sh_adapter=split_adapter,
        sz_adapter=convert_adapter,
        inst=inst,
    )
    symbol_router_engine = SymbolRouterEngine()

    symbol_router_adapter = SymbolRouterAdapter(engine=symbol_router_engine, inst=inst, symbols=cfg.data.symbols)

    symbol_step = SymbolSplitStep(adapter=symbol_router_adapter, inst=inst)

    normalize_engine = NormalizeEngine()
    norm_adapter = NormalizeAdapter(engine=normalize_engine, inst=inst, symbols=cfg.data.symbols)
    normalize_step = NormalizeStep(adapter=norm_adapter, inst=inst)

    enricher = TradeEnrichEngine()
    enricher_adapter = TradeEnrichAdapter(engine=enricher, inst=inst, symbols=cfg.data.symbols)

    order_engine = OrderBookRebuildEngine()
    order_adapter = OrderBookRebuildAdapter(engine=order_engine, inst=inst, symbols=cfg.data.symbols)
    order_step = OrderBookRebuildStep(adapter=order_adapter, inst=inst)

    # ----------- Step Layer -----------
    steps = [
        DownloadStep(down_adapter, inst=inst),
        csv_convert_step,
        symbol_step,
        normalize_step,
        TradeEnrichStep(enricher_adapter,  inst=inst),
        order_step
    ]

    return DataPipeline(steps, pm, inst)
