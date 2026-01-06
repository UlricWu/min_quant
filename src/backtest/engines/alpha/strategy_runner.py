from typing import Dict

import numpy as np


class StrategyRunner:
    """
    StrategyRunner (FINAL)

    Responsibilities:
    - Glue code between Engine and Strategy
    - Delegates prediction to strategy.model
    - Delegates decision to strategy.decide
    """

    def __init__(self, *, strategy, symbols):
        assert hasattr(strategy, "model"), "Strategy must own an inference model"

        self.strategy = strategy
        self.symbols = symbols

    def on_minute(self, *, ts_us: int, data_view, portfolio) -> Dict[str, int]:
        # --------------------------------------------------
        # 1. Collect features
        # --------------------------------------------------
        features_by_symbol = {
            s: data_view.get_features(s)
            for s in self.symbols
        }

        # --------------------------------------------------
        # 2. Predict scores via strategy-owned model
        # --------------------------------------------------
        scores = self.strategy.model.predict(features_by_symbol)
        # --------------------------------------------------
        # 3. Let strategy decide target positions
        # --------------------------------------------------
        target_qty = self.strategy.decide(
            ts_us=ts_us,
            scores=scores,
            portfolio=portfolio,
        )

        return target_qty
