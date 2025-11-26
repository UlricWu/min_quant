#!filepath: src/config/model_config.py
from pydantic import BaseModel

class ModelConfig(BaseModel):
    train_days: int = 120
    valid_days: int = 20
    test_days: int = 20
    model_path: str = "models/"
