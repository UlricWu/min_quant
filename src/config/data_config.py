#!filepath: src/config/data_config.py
from pydantic import BaseModel
from typing import Dict


class DataConfig(BaseModel):
    remote_dir: str