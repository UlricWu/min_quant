# !filepath: src/backtest/strategy/policies/long_only.py
from typing import Dict
from src.backtest.strategy.base import BaseStrategy
from src.backtest.strategy.registry import register_strategy


@register_strategy("long_only")
class LongOnlyStrategy(BaseStrategy):
    def __init__(self, threshold: float = 0.0, qty: int = 100):
        self.threshold = threshold
        self.qty = qty

    def decide(self, *, ts_us: int, scores: Dict[str, float], portfolio) -> Dict[str, int]:
        target = {}
        for sym, score in scores.items():
            target[sym] = self.qty if score > self.threshold else 0
        return target
