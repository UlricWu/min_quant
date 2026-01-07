from __future__ import annotations

from abc import ABC, abstractmethod
import pandas as pd
from typing import Any


class ModelTrainEngine(ABC):
    """
    Abstract ModelTrainEngine (FINAL)
    """

    def __init__(self, cfg):
        self.cfg = cfg

    @abstractmethod
    def train(
        self,
        *,
        X: pd.DataFrame,
        y: pd.Series,
        prev_model: Any | None,
    ) -> Any:
        """
        Returns updated model
        """
        raise NotImplementedError
