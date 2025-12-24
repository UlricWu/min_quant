# src/pipeline/parallel/types.py
from enum import Enum


class ParallelKind(str, Enum):
    FILE = "file"
    SYMBOL = "symbol"
