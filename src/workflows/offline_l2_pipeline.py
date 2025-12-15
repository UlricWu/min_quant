#!filepath: src/workflows/offline_l2_pipeline.py
from __future__ import annotations

from src.dataloader.pipeline.pipeline import DataPipeline
from src.utils.path import PathManager
from src.config.app_config import AppConfig

# steps
from src.dataloader.pipeline.steps.download_step import DownloadStep
from src.dataloader.pipeline.steps.csv_to_parquet_step import CsvConvertStep

# adapter
from src.adapters.convert_adapter import ConvertAdapter, SplitConvertAdapter
from src.adapters.ftp_download_adapter import FtpDownloadAdapter

# engine
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.engines.ftp_download_engine import FtpDownloadEngine

from src.observability.instrumentation import Instrumentation
from src.engines.WriterEngine import StreamingWriterEngine

from src.engines.extractor_engine import ExtractorEngine
from src.engines.event_splitter_engine import TickTypeSplitterEngine


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
    down_adapter = FtpDownloadAdapter(
        host=cfg.secret.ftp_host,
        user=cfg.secret.ftp_user,
        password=cfg.secret.ftp_password,
        port=cfg.secret.ftp_port,
        engine=down_engine,
        inst=inst,
    )

    tick = TickTypeSplitterEngine()
    writer = StreamingWriterEngine()

    convert_adapter = ConvertAdapter(
        extractor=ExtractorEngine,
        writer=writer,

    )

    split_adapter = SplitConvertAdapter(extractor=ExtractorEngine,
                                        writer=writer,
                                        splitter=tick
                                        )

    csv_convert_step = CsvConvertStep(
        sh_adapter=split_adapter,
        sz_adapter=convert_adapter,
        inst=inst,
    )

    # trade_adapter = TradeEnrichAdapter(
    #     engine=trade_engine,
    #     pm=pm,
    #     symbols=cfg.data.symbols,
    #     inst=inst,
    # )
    #
    # symbol_router_adapter = SymbolRouterAdapter(
    #     symbols=cfg.data.symbols,
    #     pm=pm,
    #     inst=inst,
    # )

    # ----------- Step Layer -----------
    steps = [
        DownloadStep(down_adapter, inst=inst),
        csv_convert_step
        # SymbolSplitStep(symbol_router_adapter, inst=inst),
        # TradeEnrichStep(trade_adapter, inst=inst),
    ]

    return DataPipeline(steps, pm, inst)
