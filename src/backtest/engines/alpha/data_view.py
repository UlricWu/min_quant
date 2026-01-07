from __future__ import annotations

from typing import Dict, Any, Optional

import pyarrow as pa
import pyarrow.compute as pc

from src.backtest.core.data import MarketDataView
from src.meta.symbol_slice_resolver import SymbolSliceResolver


class MinuteFeatureDataView(MarketDataView):
    """
    MinuteFeatureDataView (FINAL / FROZEN)

    FROZEN RULES:
    - DataView is a FACT VIEW, not a feature-policy enforcer.
    - It MUST NOT raise on NaN/inf; it returns raw values.
    - Feature policy (fill/drop/clip) belongs to Strategy/InferenceModel.
    """

    def __init__(
            self,
            *,
            resolver: SymbolSliceResolver,
            symbols: list[str],
            feature_names: list[str],
            ts_col: str = "ts",
            price_col: str = "close",  # <- ensure this matches your table
    ):
        self._resolver = resolver
        self._symbols = list(symbols)
        self._feature_names = list(feature_names)
        self._ts_col = ts_col
        self._price_col = price_col

        self._tables: Dict[str, pa.Table] = resolver.get_many(symbols)
        self._current_ts: Optional[int] = None

        self._validate_schema()
        self._min_ts_us, self._max_ts_us = self._compute_time_bounds()

    def on_time(self, ts_us: int) -> None:
        self._current_ts = int(ts_us)

    def get_price(self, symbol: str) -> Optional[float]:
        row = self._locate_last_row(symbol)
        if row is None:
            return None
        v = row[self._price_col].as_py()
        return None if v is None else float(v)

    def get_features(self, symbol: str) -> Dict[str, Any]:
        """
        Return raw feature values for current time snapshot.

        FINAL:
        - returns {} when no row available
        - returns raw values including None/NaN/inf (no validation, no filling)
        """
        row = self._locate_last_row(symbol)
        if row is None:
            return {}

        feats: Dict[str, Any] = {}
        for name in self._feature_names:
            # raw as_py: may be None, nan, inf; that's allowed
            feats[name] = row[name].as_py()
        return feats

    # -------------------------
    # Internals
    # -------------------------
    def _locate_last_row(self, symbol: str) -> Optional[pa.StructScalar]:
        if self._current_ts is None:
            raise RuntimeError("on_time(ts_us) must be called before data access")

        table = self._tables[symbol]
        ts_arr = table[self._ts_col]
        mask = pc.less_equal(ts_arr, pa.scalar(self._current_ts))
        filtered = table.filter(mask)

        if filtered.num_rows == 0:
            return None
        return filtered.slice(filtered.num_rows - 1, 1).to_struct_array()[0]

    def _validate_schema(self) -> None:
        for symbol, table in self._tables.items():
            if self._ts_col not in table.schema.names:
                raise RuntimeError(
                    f"[MinuteFeatureDataView] missing ts_col={self._ts_col} for symbol={symbol}"
                )
            if self._price_col not in table.schema.names:
                raise RuntimeError(
                    f"[MinuteFeatureDataView] missing price_col={self._price_col} for symbol={symbol}"
                )

            missing = {c for c in self._feature_names if c not in table.schema.names}
            if missing:
                raise RuntimeError(
                    f"[MinuteFeatureDataView] missing features={sorted(missing)} for symbol={symbol}"
                )

    def _compute_time_bounds(self) -> tuple[int, int]:
        mins = []
        maxs = []
        for table in self._tables.values():
            col = table[self._ts_col]
            mins.append(pc.min(col).as_py())
            maxs.append(pc.max(col).as_py())
        return int(min(mins)), int(max(maxs))

    # --------------------------------------------------
    # Time bounds (Clock contract)
    # --------------------------------------------------
    def time_bounds_us(self) -> tuple[int, int]:
        """
        Return (min_ts_us, max_ts_us) across all symbols.

        FINAL CONTRACT:
        - Values are inclusive
        - Clock is responsible for stepping policy
        """
        return self._min_ts_us, self._max_ts_us
