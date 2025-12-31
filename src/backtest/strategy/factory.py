# src/backtest/strategy/factory.py
from __future__ import annotations

from typing import Dict, Type

from src.backtest.strategy.base import Strategy
from src.backtest.strategy.threshold import ThresholdStrategy


class StrategyFactory:
    """
    StrategyFactory (FINAL / FROZEN)

    注册式 Strategy 构造器
    Strategy Registration (Frozen)

    All strategies must be explicitly registered in StrategyFactory._REGISTRY.

    Registration is centralized and static.

    No dynamic discovery or side-effect-based registration is allowed.

    Adding a strategy requires a deliberate code change in the factory.
    """

    _REGISTRY: Dict[str, Type[Strategy]] = {
        "threshold": ThresholdStrategy,
        # "ma_cross": MACrossStrategy,
        # 未来只在这里注册
    }

    # --------------------------------------------------
    @classmethod
    def create(cls, cfg: Dict) -> Strategy:
        """
        cfg:
          backtest.strategy（完整 dict）

        冻结规则：
          - cfg["type"] 必须存在
          - 未注册 type -> crash
        """
        if "type" not in cfg:
            raise KeyError("[StrategyFactory] missing 'type' in strategy config")

        typ = cfg["type"]

        if typ not in cls._REGISTRY:
            raise ValueError(
                f"[StrategyFactory] unknown strategy type: {typ}"
            )

        strategy_cls = cls._REGISTRY[typ]

        # type 字段不传给 Strategy 本体
        params = {k: v for k, v in cfg.items() if k != "type"}

        return strategy_cls(**params)
