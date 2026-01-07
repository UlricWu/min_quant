# src/backtest/engines/production/data_view.py
from backtest.core.data import MarketDataView


class LiveMarketDataView(MarketDataView):
    def on_time(self, ts_us: int) -> None:
        pass

    def get_price(self, symbol: str):
        return None

    def get_features(self, symbol: str):
        return {}
