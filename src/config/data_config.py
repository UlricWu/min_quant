#!filepath: src/config/data_config.py
from pydantic import BaseModel
from typing import Dict

class SchemaConfig(BaseModel):
    symbol: str
    date: str
    price: str
    volume: str

class DataConfig(BaseModel):
    remote_dir: str
    local_raw: str
    parquet_root: str
    schema: SchemaConfig
