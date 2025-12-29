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

from src.engines.feature_l0_engine import FeatureL0Engine
from src.engines.feature_l1_norm_engine import FeatureL1NormEngine
from src.engines.feature_l1_stat_engine import FeatureL1StatEngine

from src.steps.feature_build_step import FeatureBuildStep

from src.steps.label_build_step import LabelBuildStep
from src.engines.labels.forward_return_label_engine import ForwardReturnLabelEngine


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

    feature_step = FeatureBuildStep(
        l0_engine=FeatureL0Engine(),
        l1_engines=[
            FeatureL1StatEngine(window=20),
            FeatureL1NormEngine(window=20),
            # FeatureL1StatEngine(window=60),
            # FeatureL1NormEngine(window=60),
        ],
        l2_engine=None,
        only_feature_columns=True,  # 强烈建议打开，防止覆盖 open/high/low/close
        inst=inst
    )

    # ❗ 注意：steps 是“行位移”，不是分钟
    pipeline_cfg = cfg.pipeline

    label_engine = ForwardReturnLabelEngine(
        steps=pipeline_cfg.horizon,
        price_col=pipeline_cfg.price_col,
        use_log_return=pipeline_cfg.use_log_return,
    )

    label_step = LabelBuildStep(
        engine=label_engine,
        inst=inst,
    )

    steps = [
        # download_step,
        extractor_steps,
        normalize_steps,
        # 成交主线（稳定、低成本）
        trade_step,
        min_trade_step,
        feature_step,
        label_step
        # 盘口侧线（按需启用）
        # order_step,
        # ordertrade_step,
        # orderbook_rebuild_step,
        # ob_feature_step,
    ]

    return DataPipeline(
        steps=steps,
        pm=pm,
        inst=inst,
    )
