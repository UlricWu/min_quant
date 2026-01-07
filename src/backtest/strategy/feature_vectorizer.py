from __future__ import annotations
from typing import Dict, Any, List
import numpy as np
import pandas as pd


class FeatureVectorizer:
    """
    FeatureVectorizer (FINAL / FROZEN)

    Responsibility:
    - Convert execution-domain features_by_symbol
      into a 2D matrix aligned with training feature order.

    Contract:
    - Missing features -> NaN
    - Feature order is FROZEN and provided at construction
    """

    def __init__(self, feature_names: List[str]):
        self.feature_names = list(feature_names)

    def transform(
        self,
        features_by_symbol: Dict[str, Dict[str, Any]],
    ) -> tuple[pd.DataFrame, list[str]]:
        """
        Returns:
            X : DataFrame (n_symbols, n_features)
            symbols : list[str] (row order)
        """
        symbols = list(features_by_symbol.keys())

        rows = []
        for sym in symbols:
            feats = features_by_symbol[sym]
            row = [feats.get(f, np.nan) for f in self.feature_names]
            rows.append(row)

        X = pd.DataFrame(rows, columns=self.feature_names, index=symbols)
        return X, symbols
