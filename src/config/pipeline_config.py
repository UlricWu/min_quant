# src/config/pipeline_config.py
from pydantic import BaseModel
from enum import Enum


class DownloadBackend(str, Enum):
    FTPLIB = "ftplib"
    CURL = "curl"


class PipelineConfig(BaseModel):
    ftp_backend: DownloadBackend = DownloadBackend.CURL
