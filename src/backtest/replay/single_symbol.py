# src/backtest/replay/single_symbol.py
from __future__ import annotations

from src.backtest.replay.base import ReplayPolicy
from src.backtest.data.feature_data_handler import FeatureDataHandler


class SingleSymbolReplay(ReplayPolicy):
    def __init__(self, handler: FeatureDataHandler) -> None:
        self._handler = handler

    def replay(self):
        yield from self._handler.iter_events()
