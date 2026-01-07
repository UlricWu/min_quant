from __future__ import annotations

"""
{#!filepath: src/backtest/strategy/base.py}

Strategy Base Contracts (FINAL / FROZEN)

Defines the minimal interfaces for Model and Strategy.

Invariants:
- Strategy decides target positions based on observable facts.
- Model produces scores from feature snapshots.
- Neither knows about data source, replay, or execution mechanics.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class Strategy(ABC):
    """
    Strategy (FINAL / FROZEN)

    Contract:
    - Strategy MUST be constructed with an inference model
    - Strategy owns decision logic only
    """

    def __init__(self, model: InferenceModel):
        self.model = model

    @abstractmethod
    def decide(self, ts_us, scores, portfolio):
        ...


class InferenceModel:
    """
    Inference-only model (FINAL)
    """

    def predict(self, X):
        raise NotImplementedError


class Model(ABC):
    """
    Base Model interface (FINAL).

    A Model:
    - consumes per-symbol feature dict
    - outputs per-symbol scores
    """

    @abstractmethod
    def predict(
            self,
            features_by_symbol: Dict[str, Dict[str, Any]],
    ) -> Dict[str, float]:
        raise NotImplementedError
