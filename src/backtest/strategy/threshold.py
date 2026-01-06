from __future__ import annotations

import numpy as np

from src import logs

"""
{#!filepath: src/backtest/strategy/threshold.py}

Threshold Strategy (FINAL)

A simple reference strategy:
- score > threshold  -> long 1
- otherwise          -> flat 0
"""

from typing import Dict, Any

from src.backtest.strategy.base import Strategy, InferenceModel


class ThresholdStrategy(Strategy):
    """
    Threshold-based long-only strategy (FINAL).
    """

    def __init__(self, model, threshold: float = 0.0, qty: int = 1):
        self.model = model
        self.threshold = float(threshold)
        self.qty = int(qty)
        self._logged = False

    def decide(self, ts_us, scores, portfolio):
        if not self._logged:
            self._logged = True
            logs.info(f"[ThresholdStrategy] threshold={self.threshold} qty={self.qty}")
            # 打印两个 symbol 的分数看看
            for k, v in list(scores.items())[:2]:
                logs.info(f"[ThresholdStrategy] sample score {k}={np.clip(v, -10.0, 10.0)}")
        target = {s: (self.qty if score > self.threshold else 0) for s, score in scores.items()}
        return target
