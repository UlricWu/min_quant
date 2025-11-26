#!filepath: src/config/app_config.py
import yaml
from pydantic import BaseModel

from .log_config import LogConfig
from .data_config import DataConfig
from .model_config import ModelConfig
from .pipeline_config import PipelineConfig


class AppConfig(BaseModel):
    log: LogConfig
    data: DataConfig
    model: ModelConfig
    pipeline: PipelineConfig

    @classmethod
    def load(cls, path: str = "config/base.yaml") -> "AppConfig":
        """加载 YAML 配置并转换成 AppConfig 对象"""
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        return cls(**raw)
