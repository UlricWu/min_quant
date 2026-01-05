# src/workflows/offline_training.py
from __future__ import annotations

from src.config.app_config import AppConfig
from src.observability.instrumentation import Instrumentation
from src.utils.path import PathManager
from src.training.pipeline import TrainingPipeline

from src.training.steps.dataset_build_step import DatasetBuildStep
from src.training.steps.model_train_step import ModelTrainStep
from src.training.steps.model_evaluate_step import ICEvaluateStep
from src.training.steps.ic_series_report_step import ICSeriesReportStep

def build_offline_training() -> TrainingPipeline:
    """
    Offline Training Workflow (FINAL / FROZEN)
    """

    cfg = AppConfig.load().training
    pm = PathManager()
    inst = Instrumentation()

    return TrainingPipeline(
        daily_steps=[
            DatasetBuildStep(),
            ModelTrainStep(cfg),
            ICEvaluateStep(pm),
        ],
        final_steps=[
            ICSeriesReportStep(
                pm=pm,
                rolling_window=20,
            )
        ],
        pm=pm,
        inst=inst,
        cfg=cfg,
    )
