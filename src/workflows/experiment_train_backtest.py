# src/workflows/experiment.py
from __future__ import annotations

from src.workflows.offline_training import build_offline_training
from src.workflows.offline_l1_backtest import build_offline_l1_backtest
from src.workflows.experiment_pipeline import ExperimentPipeline


def run_train_then_backtest():
    """
    Build Experiment Pipeline (FINAL / FROZEN)

    NOTE:
    - This function MUST NOT call .run()
    - It ONLY wires pipelines
    """
    return ExperimentPipeline(
        training_pipeline=build_offline_training(),
        backtest_pipeline=build_offline_l1_backtest(),
    )
