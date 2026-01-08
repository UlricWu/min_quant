# src/workflows/offline_training.py
from __future__ import annotations

from src.config.app_config import AppConfig
from src.observability.instrumentation import Instrumentation
from src.utils.path import PathManager
from src.training.pipeline import TrainingPipeline

from src.training.steps.dataset_build_step import DatasetBuildStep
from src.training.steps.model_train_step import ModelTrainStep
from src.training.steps.model_evaluate_step import ICEvaluateStep
from src.training.steps.rank_ic_step import RankICStep
from src.training.engines.ic_evaluate_engine import ICEvaluateEngine
from src.training.steps.rank_ic_series_report_step import RankICSeriesReportStep
from src.training.engines.rank_ic_evaluate_engine import RankICEvaluateEngine

from src.training.steps.artifact_persist_step import ArtifactPersistStep


def build_offline_training(cfg=None) -> TrainingPipeline:
    """
    Offline Training Workflow (FINAL / FROZEN)
    """

    if cfg is None:
        cfg = AppConfig.load().training
    pm = PathManager()
    inst = Instrumentation()

    return TrainingPipeline(
        daily_steps=[
            DatasetBuildStep(pm=pm, inst=inst),
            ModelTrainStep(cfg),
            ICEvaluateStep(ICEvaluateEngine()),
        ],
        final_steps=[
            RankICStep(RankICEvaluateEngine()),
            RankICSeriesReportStep(rolling_window=cfg.warmup_days),
            ArtifactPersistStep(),
        ],
        pm=pm,
        inst=inst,
        cfg=cfg,
    )
