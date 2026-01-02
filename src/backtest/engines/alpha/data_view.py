from __future__ import annotations

from typing import Dict, Any, Optional

import pyarrow as pa
import pyarrow.compute as pc

from src.backtest.core.data import MarketDataView
from src.meta.symbol_slice_resolver import SymbolSliceResolver


class MinuteFeatureDataView(MarketDataView):
    """
    MinuteFeatureDataView (FINAL)

    Worldview:
    - Data is immutable Arrow Table (fact)
    - Time moves via on_time(ts_us)
    - At each time, expose observable snapshot:
        - price (last)
        - features (l1*)
    """

    def __init__(
        self,
        *,
        resolver: SymbolSliceResolver,
        symbols: list[str],
        ts_col: str = "ts",
        price_col: str = "last",
        feature_prefix: str = "l1_",
    ):
        self._resolver = resolver
        self._symbols = list(symbols)
        self._ts_col = ts_col
        self._price_col = price_col
        self._feature_prefix = feature_prefix

        # --------------------------------------------------
        # Load Arrow tables once (immutable facts)
        # --------------------------------------------------
        self._tables: Dict[str, pa.Table] = resolver.get_many(symbols)

        # Current time cursor
        self._current_ts: Optional[int] = None

    # --------------------------------------------------
    # Time progression
    # --------------------------------------------------
    def on_time(self, ts_us: int) -> None:
        self._current_ts = int(ts_us)

    # --------------------------------------------------
    # Observable price
    # --------------------------------------------------
    def get_price(self, symbol: str) -> Optional[float]:
        row = self._locate_last_row(symbol)
        if row is None:
            return None
        return float(row[self._price_col].as_py())

    # --------------------------------------------------
    # Observable features
    # --------------------------------------------------
    def get_features(self, symbol: str) -> Dict[str, Any]:
        row = self._locate_last_row(symbol)
        if row is None:
            return {}

        feats: Dict[str, Any] = {}
        for col in row.schema.names:
            if col.startswith(self._feature_prefix):
                feats[col] = row[col].as_py()
        return feats

    # --------------------------------------------------
    # Internals
    # --------------------------------------------------
    def _locate_last_row(self, symbol: str) -> Optional[pa.StructScalar]:
        """
        Locate the latest row with ts <= current_ts.
        """
        if self._current_ts is None:
            raise RuntimeError("on_time(ts_us) must be called before data access")

        table = self._tables[symbol]
        ts_arr = table[self._ts_col]

        # Boolean mask: ts <= current_ts
        mask = pc.less_equal(ts_arr, pa.scalar(self._current_ts))
        filtered = table.filter(mask)

        if filtered.num_rows == 0:
            return None

        # Return last row (StructScalar)
        return filtered.slice(filtered.num_rows - 1, 1).to_struct_array()[0]
