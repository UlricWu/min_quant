from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Any
# src/backtest/core/types.py

@dataclass(frozen=True)
class FeatureSnapshot:
    symbol: str
    values: Dict[str, Any]
