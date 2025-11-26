#!filepath: src/config/pipeline_config.py
from pydantic import BaseModel

class PipelineConfig(BaseModel):
    enable_download: bool = True
    enable_decompress: bool = True
    enable_parse: bool = True
    enable_write: bool = True
