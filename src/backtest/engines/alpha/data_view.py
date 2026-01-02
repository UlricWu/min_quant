from __future__ import annotations

from typing import Dict, Any, Optional

from src.backtest.core.data import MarketDataView


class DummyMinuteDataView(MarketDataView):
    """
    DummyMinuteDataView (MVP)

    Purpose:
    - Satisfy MarketDataView contract
    - Allow Engine A to run end-to-end
    - No real data semantics

    This class exists ONLY to validate architecture wiring.
    """

    def __init__(self, symbols):
        self.symbols = list(symbols)
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
        # Constant dummy price
        return 10.0

    # --------------------------------------------------
    # Observable features
    # --------------------------------------------------
    def get_features(self, symbol: str) -> Dict[str, Any]:
        """
        Return a per-symbol feature snapshot at current time.

        Dummy implementation:
        - Always returns a constant feature vector
        - Key names mimic real L1 features
        """
        if self._current_ts is None:
            raise RuntimeError("on_time() must be called before get_features()")

        return {
            "l1_dummy": 0.0,
            "ts_us": self._current_ts,
        }
