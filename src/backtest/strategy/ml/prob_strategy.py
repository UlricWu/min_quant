from __future__ import annotations

"""
{#!filepath: src/backtest/strategy/ml/prob_strategy.py}

Probability-based Strategy (FINAL)

Turns model probabilities into target positions.
"""

from typing import Dict

from src.backtest.strategy.base import Strategy


class ProbabilityThresholdStrategy(Strategy):
    def __init__(self, threshold: float = 0.5, qty: int = 1):
        self.threshold = threshold
        self.qty = qty

    def decide(self, ts_us, scores, portfolio) -> Dict[str, int]:
        target = {}
        for symbol, prob in scores.items():
            target[symbol] = self.qty if prob >= self.threshold else 0
        return target
