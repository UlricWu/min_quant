#!filepath: tests/test_app_config.py
import yaml
import pytest
from pathlib import Path

from src.config import AppConfig
from src.config.log_config import LogConfig
from src.config.data_config import DataConfig
from src.config.model_config import ModelConfig
from src.config.pipeline_config import PipelineConfig


@pytest.fixture
def sample_config_file(tmp_path):
    """
    创建临时 YAML 配置文件用于测试，
    pytest 会自动清理该目录。
    """
    data = {
        "log": {
            "dir": "logs",
            "rotation": "1 day",
            "retention": "30 days",
            "level": "DEBUG"
        },
        "data": {
            "remote_dir": "/ftp/level2/",
            "local_raw": "dataloader/raw/",
            "parquet_root": "dataloader/parquet/",
            "symbols": ['603322', '002594']
        },
        "model": {
            "train_days": 100,
            "valid_days": 20,
            "test_days": 20,
            "model_path": "models/"
        },
        "pipeline": {
            "enable_download": True,
            "enable_decompress": False,
            "enable_parse": True,
            "enable_write": True
        }
    }

    config_file = tmp_path / "config.yaml"
    config_file.write_text(yaml.safe_dump(data), encoding="utf-8")
    return config_file


def test_app_config_load(sample_config_file):
    """测试 AppConfig 是否能正确加载 YAML"""
    cfg = AppConfig.load(path=str(sample_config_file))

    # 顶层
    assert isinstance(cfg, AppConfig)

    # 子配置是否根据 schema 解析成功
    assert isinstance(cfg.log, LogConfig)
    assert isinstance(cfg.data, DataConfig)
    assert isinstance(cfg.model, ModelConfig)
    assert isinstance(cfg.pipeline, PipelineConfig)


def test_log_config_values(sample_config_file):
    """检查 log 配置内容是否正确读取"""
    cfg = AppConfig.load(path=str(sample_config_file))
    assert cfg.log.level == "DEBUG"
    assert cfg.log.dir == "logs"


def test_data_config_values(sample_config_file):
    """检查 dataloader 配置内容是否正确读取"""
    cfg = AppConfig.load(path=str(sample_config_file))

    assert cfg.data.remote_dir == "/ftp/level2/"
    # assert cfg.dataloader.schema.price== "float"
    # assert "symbol" in cfg.dataloader.schema


def test_model_config_defaults(sample_config_file):
    """测试 model config 是否正常读取"""
    cfg = AppConfig.load(path=str(sample_config_file))

    assert cfg.model.train_days == 100
    assert cfg.model.model_path == "models/"


def test_pipeline_config_values(sample_config_file):
    """检查 pipeline 配置内容是否读取正确"""
    cfg = AppConfig.load(path=str(sample_config_file))

    assert cfg.pipeline.enable_download is True
    assert cfg.pipeline.enable_decompress is False


def test_missing_field_should_fail(tmp_path):
    """当配置缺少字段时，AppConfig 应该抛出 ValidationError"""
    bad_data = {
        "log": {
            "dir": "logs"
        }
        # dataloader/model/pipeline 缺失
    }

    bad_file = tmp_path / "bad.yaml"
    bad_file.write_text(yaml.safe_dump(bad_data))

    with pytest.raises(Exception):
        AppConfig.load(path=str(bad_file))
