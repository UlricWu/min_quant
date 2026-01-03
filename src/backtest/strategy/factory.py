from __future__ import annotations

"""
{#!filepath: src/backtest/strategy/factory.py}

StrategyFactory (FINAL / FROZEN)

Static, centralized Strategy Factory.

Design rationale:
- The set of available strategies is treated as an explicit capability whitelist.
- All strategies MUST be deliberately registered here.
- No dynamic discovery, decorators, or side-effect-based registration is allowed.

This is a conscious design choice for the current system phase:
- Research-oriented
- Low strategy churn
- High auditability and clarity

A decorator-based registry is a possible future evolution,
but is intentionally NOT used at this stage.
"""

from typing import Dict, Tuple, Type

from src.backtest.strategy.base import Strategy, Model
from src.backtest.strategy.threshold import ThresholdModel, ThresholdStrategy
from src.backtest.strategy.ml.rf_model import RandomForestModel
from src.backtest.strategy.ml.prob_strategy import ProbabilityThresholdStrategy


class StrategyFactory:
    """
    Static Strategy Factory (FINAL).

    Responsibilities:
    - Validate strategy config
    - Construct (Model, Strategy) pair
    - Enforce explicit capability whitelist
    """

    # --------------------------------------------------
    # Explicit capability registry (STATIC / FROZEN)
    # --------------------------------------------------
    _REGISTRY: Dict[str, Tuple[Type[Model], Type[Strategy]]] = {
        "threshold": (ThresholdModel, ThresholdStrategy),
        "rf_prob": (RandomForestModel, ProbabilityThresholdStrategy)
        # "ma_cross": (MACrossModel, MACrossStrategy),
        # Future strategies MUST be added explicitly here.
    }

    # --------------------------------------------------
    @classmethod
    def build(cls, cfg: Dict) -> Tuple[Model, Strategy]:
        """
        Build (Model, Strategy) from cfg.strategy.

        Expected cfg format:
        {
            "type": "threshold",
            "model": { ... },
            "params": { ... }
        }

        Frozen rules:
        - cfg["type"] MUST exist
        - type MUST be registered
        - Unknown type -> hard failure
        """

        if "type" not in cfg:
            raise KeyError("[StrategyFactory] missing 'type' in strategy config")

        typ = cfg["type"]

        if typ not in cls._REGISTRY:
            raise ValueError(f"[StrategyFactory] unknown strategy type: {typ}")

        model_cls, strategy_cls = cls._REGISTRY[typ]

        model_cfg = cfg.get("model", {})
        strategy_cfg = cfg.get("params", {})

        model = model_cls(**model_cfg)
        strategy = strategy_cls(**strategy_cfg)

        return model, strategy
