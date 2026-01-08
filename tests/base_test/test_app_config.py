#!filepath: tests/test_app_config.py
from __future__ import annotations

import yaml
import pytest
from pathlib import Path

from src.config import AppConfig
from src.config.log_config import LogConfig
from src.config.data_config import DataConfig
from src.config.model_config import ModelConfig
from src.config.pipeline_config import PipelineConfig
from src.config.backtest_config import BacktestConfig
from src.config.training_config import TrainingConfig


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def sample_config_file(tmp_path: Path) -> Path:
    """
    Minimal but COMPLETE declarative config.

    IMPORTANT:
    - This config is declarative ONLY
    - No execution semantics are validated here
    """
    data = {
        # -----------------------------
        # logging
        # -----------------------------
        "log": {
            "dir": "logs",
            "rotation": "1 day",
            "retention": "30 days",
            "level": "DEBUG",
        },

        # -----------------------------
        # data
        # -----------------------------
        "data": {
            "remote_dir": "level2",
        },

        # -----------------------------
        # model (legacy/global defaults)
        # -----------------------------
        "model": {
            "train_days": 100,
            "valid_days": 20,
            "test_days": 20,
            "model_path": "models/",
        },

        # -----------------------------
        # pipeline
        # -----------------------------
        "pipeline": {
            "ftp_backend": "curl",
            "max_worker": 4,
        },

        # -----------------------------
        # backtest (DECLARATIVE)
        # -----------------------------
        "backtest": {
            "name": "dummy_alpha",
            "level": "l1",
            "replay": "single",
            "dates": ["2025-12-01"],
            "symbols": ["600001"],
            "strategy": {
                "type": "threshold",
                "model": {
                    "spec": {
                        "family": "sgd",
                        "task": "regression",
                        "version": "v1",
                        "artifact": {
                            "run": "minute_sgd_online_v1",
                            "asof": "latest",
                        },
                    }
                },
                "params": {
                    "threshold": -0.0005,
                    "qty": 10,
                },
                "signal_feature": "close",
            },
        },

        # -----------------------------
        # training (DECLARATIVE)
        # -----------------------------
        "training": {
            "name": "minute_sgd_online_v1",
            "start_date": "2025-11-01",
            "end_date": "2025-11-05",
            "warmup_days": 1,
            "model_name": "sgd",
            "model_version": "v1",
            "task_type": "regression",
            "model_params": {
                "alpha": 0.0005,
                "l1_ratio": 0.0,
            },
            "evaluation_enabled": True,
            "evaluation_metrics": ["ic"],
            "snapshot_enabled": False,
            "dataset": {
                "feature_columns": ["open", "high", "low", "close", "volume"],
                "label_column": "label_fwd_ret_s5",
                "drop_na": True,
            },
        },
    }

    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.safe_dump(data), encoding="utf-8")
    return config_file


# =============================================================================
# Tests
# =============================================================================

def test_app_config_load(sample_config_file: Path):
    """
    AppConfig should load declarative config successfully.
    """
    cfg = AppConfig.load(path=str(sample_config_file))

    assert isinstance(cfg, AppConfig)
    assert isinstance(cfg.log, LogConfig)
    assert isinstance(cfg.data, DataConfig)
    assert isinstance(cfg.model, ModelConfig)
    assert isinstance(cfg.pipeline, PipelineConfig)
    assert isinstance(cfg.backtest, BacktestConfig)
    assert isinstance(cfg.training, TrainingConfig)


def test_log_config_values(sample_config_file: Path):
    cfg = AppConfig.load(path=str(sample_config_file))
    assert cfg.log.level == "DEBUG"
    assert cfg.log.dir == "logs"


def test_data_config_values(sample_config_file: Path):
    cfg = AppConfig.load(path=str(sample_config_file))
    assert cfg.data.remote_dir == "level2"


def test_model_config_values(sample_config_file: Path):
    cfg = AppConfig.load(path=str(sample_config_file))
    assert cfg.model.train_days == 100
    assert cfg.model.model_path == "models/"


def test_pipeline_config_values(sample_config_file: Path):
    cfg = AppConfig.load(path=str(sample_config_file))
    assert cfg.pipeline.ftp_backend == "curl"
    assert cfg.pipeline.max_worker == 4


def test_backtest_config_declarative_only(sample_config_file: Path):
    """
    BacktestConfig is declarative:
    - strategy remains a dict
    - no artifact resolution happens here
    """
    cfg = AppConfig.load(path=str(sample_config_file))
    bt = cfg.backtest

    assert bt.name == "dummy_alpha"
    # assert bt.level == "l1"
    assert bt.symbols == ["600001"]
    assert isinstance(bt.strategy, dict)
    assert bt.strategy["type"] == "threshold"


def test_training_config_values(sample_config_file: Path):
    cfg = AppConfig.load(path=str(sample_config_file))
    tr = cfg.training

    assert tr.name == "minute_sgd_online_v1"
    assert tr.model_name == "sgd"
    assert tr.task_type == "regression"
    assert tr.dataset.label_column == "label_fwd_ret_s5"


def test_missing_required_field_should_fail(tmp_path: Path):
    """
    Missing REQUIRED top-level fields must fail fast.

    This validates schema strictness, not execution logic.
    """
    bad_data = {
        "log": {
            "dir": "logs",
        }
        # data / model / pipeline / backtest / training missing
    }

    bad_file = tmp_path / "bad.yaml"
    bad_file.write_text(yaml.safe_dump(bad_data), encoding="utf-8")

    with pytest.raises(Exception):
        AppConfig.load(path=str(bad_file))
