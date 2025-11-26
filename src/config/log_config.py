#!filepath: src/config/log_config.py
from pydantic import BaseModel

class LogConfig(BaseModel):
    dir: str = "logs"
    rotation: str = "1 day"
    retention: str = "30 days"
    level: str = "INFO"
