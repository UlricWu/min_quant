#!filepath: src/backtest/strategy/registry.py
from __future__ import annotations
from typing import Dict, Type, Any

from src.backtest.strategy.base import BaseModel, BaseStrategy


# ------------------------------------------------------------------
# Global registries (frozen)
# ------------------------------------------------------------------
_MODEL_REGISTRY: Dict[str, Type[BaseModel]] = {}
_STRATEGY_REGISTRY: Dict[str, Type[BaseStrategy]] = {}


# ------------------------------------------------------------------
# Registration decorators
# ------------------------------------------------------------------
def register_model(name: str):
    def _wrap(cls: Type[BaseModel]):
        _MODEL_REGISTRY[name] = cls
        return cls
    return _wrap


def register_strategy(name: str):
    def _wrap(cls: Type[BaseStrategy]):
        _STRATEGY_REGISTRY[name] = cls
        return cls
    return _wrap


# ------------------------------------------------------------------
# Factory
# ------------------------------------------------------------------
# class StrategyFactory:
#     """
#     Resolve model + strategy from cfg.strategy.
#
#     This is the ONLY place that interprets cfg.strategy.
#     """
#
#     @staticmethod
#     def build(strategy_cfg: Dict[str, Any]):
#         """
#         strategy_cfg example:
#         {
#             "type": "registry",
#             "model": {"name": "rf_v1", "params": {...}},
#             "policy": {"name": "long_only", "params": {...}}
#         }
#         """
#         if strategy_cfg.get("type") != "registry":
#             raise ValueError(f"Unsupported strategy type: {strategy_cfg}")
#
#         model_cfg = strategy_cfg.get("model", {})
#         strat_cfg = strategy_cfg.get("policy", {})
#
#         model_name = model_cfg.get("name")
#         strat_name = strat_cfg.get("name")
#
#         if model_name not in _MODEL_REGISTRY:
#             raise KeyError(f"Model not registered: {model_name}")
#         if strat_name not in _STRATEGY_REGISTRY:
#             raise KeyError(f"Strategy not registered: {strat_name}")
#
#         model_cls = _MODEL_REGISTRY[model_name]
#         strat_cls = _STRATEGY_REGISTRY[strat_name]
#
#         model = model_cls(**model_cfg.get("params", {}))
#         strategy = strat_cls(**strat_cfg.get("params", {}))
#
#         return model, strategy
class StrategyFactory:
    @staticmethod
    def build(cfg):
        return _DummyModel(), _DummyStrategy()


class _DummyModel:
    def predict(self, feats):
        return {k: 0.0 for k in feats}


class _DummyStrategy:
    def decide(self, ts_us, scores, portfolio):
        return {k: 0 for k in scores}
