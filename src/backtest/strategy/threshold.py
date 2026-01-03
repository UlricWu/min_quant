from __future__ import annotations

"""
{#!filepath: src/backtest/strategy/threshold.py}

Threshold Strategy (FINAL)

A simple reference strategy:
- score > threshold  -> long 1
- otherwise          -> flat 0
"""

from typing import Dict, Any

from src.backtest.strategy.base import Strategy, Model


class ThresholdModel(Model):
    """
    Dummy scoring model.

    Produces a scalar score per symbol.
    """

    def predict(self, features_by_symbol: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
        # MVP: return zero score for all symbols
        return {symbol: 0.0 for symbol in features_by_symbol}


class ThresholdStrategy(Strategy):
    """
    Threshold-based long-only strategy.
    """

    def __init__(self, threshold: float = 0.0, qty: int = 1):
        self.threshold = threshold
        self.qty = qty

    def decide(self, ts_us, scores, portfolio):
        target = {}
        for symbol, score in scores.items():
            target[symbol] = self.qty if score > self.threshold else 0
        return target
