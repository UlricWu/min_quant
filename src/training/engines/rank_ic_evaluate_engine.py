from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import spearmanr


class RankICEvaluateEngine:
    """
    RankICEvaluateEngine（FINAL / FROZEN）

    Responsibility:
    - Compute cross-sectional Rank IC for ONE eval day

    Contract:
    - preds / y_true must be 1D aligned arrays
    - No IO, no ctx, no plotting
    """

    def evaluate(
        self,
        *,
        preds: np.ndarray,
        y_true: np.ndarray,
    ) -> float:
        if len(preds) == 0:
            return float("nan")

        ic, _ = spearmanr(preds, y_true)
        return float(ic)
