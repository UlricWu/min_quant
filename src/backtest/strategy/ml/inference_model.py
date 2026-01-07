#!filepath: src/backtest/strategy/ml/inference_model.py
from __future__ import annotations

"""
InferenceModel (FINAL / FROZEN)

InferenceModel defines HOW a trained model is used at runtime.

Responsibilities:
- Consume observable features (per symbol)
- Perform inference only (no training, no evaluation)
- Output per-symbol score or signal

Non-responsibilities:
- Training
- Hyper-parameter selection
- Dataset construction
- Decision making (strategy concern)

This abstraction lives in Backtest / Production world,
NOT in Training.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class InferenceModel(ABC):
    """
    InferenceModel (FINAL / FROZEN)

    Runtime-only model abstraction.
    """

    @abstractmethod
    def predict(
        self,
        *,
        features_by_symbol: Dict[str, Dict[str, Any]],
    ) -> Dict[str, float]:
        """
        Perform inference.

        Inputs:
        - features_by_symbol:
            {symbol -> {feature_name -> value}}

        Outputs:
        - scores:
            {symbol -> float}

        Semantics:
        - Higher score means stronger signal (engine-defined)
        """
        raise NotImplementedError
