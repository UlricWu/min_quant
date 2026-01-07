# src/config/pipeline_config.py
from pydantic import BaseModel
from enum import Enum
from typing import Optional


class DownloadBackend(str, Enum):
    FTPLIB = "ftplib"
    CURL = "curl"


class PipelineConfig(BaseModel):
    ftp_backend: DownloadBackend = DownloadBackend.CURL

    horizon: int = 5 # = steps（row offset）不是分钟，不是时间
    price_col: str = "close"
    use_log_return: bool = False
    max_worker: int = 4
