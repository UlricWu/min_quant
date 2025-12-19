#!filepath: tests/normalize/test_normalize_engine_contract.py
from __future__ import annotations

import pandas as pd

from src.engines.normalize_engine import NormalizeEngine
from src.engines.context import EngineContext


def _make_sh_order_df():
    return pd.DataFrame(
        {
            "SecurityID": ["600000", "600000"],
            "TradeTime": ["20250102", "20250102"],
            "TickTime": ["093000500", "093000000"],
            "TickType": ["A", "D"],
            "SubSeq": [2, 1],
            "Side": ["1", "2"],
            "Price": [10.1, 10.0],
            "Volume": [200, 100],
            "BuyNo": [0, 0],
            "SellNo": [0, 0],
        }
    )


def _make_sh_trade_df():
    return pd.DataFrame(
        {
            "SecurityID": ["600000", "600000"],
            "TradeTime": ["20250102", "20250102"],
            "TickTime": ["093000200", "093000100"],
            "TickType": ["T", "T"],
            "SubSeq": [10, 11],
            "Side": ["1", "2"],
            "Price": [10.05, 10.02],
            "Volume": [50, 60],
            "BuyNo": [100, 200],
            "SellNo": [300, 400],
        }
    )


def test_normalize_outputs_order_and_trade(tmp_path):
    engine = NormalizeEngine()

    symbol = "600000"
    date = "2025-01-02"
    base = tmp_path / symbol / date
    base.mkdir(parents=True)

    _make_sh_order_df().to_parquet(base / "Order.parquet", index=False)
    _make_sh_trade_df().to_parquet(base / "Trade.parquet", index=False)

    ctx = EngineContext(
        mode="offline",
        symbol=symbol,
        date=date,
        input_path=base,
        output_path=base,
    )

    engine.execute(ctx)

    order_out = base / "order" / "Normalized.parquet"
    trade_out = base / "trade" / "Normalized.parquet"

    assert order_out.exists()
    assert trade_out.exists()

    df = pd.read_parquet(order_out)

    assert list(df.columns) == [
        "symbol",
        "ts",
        "event",
        "order_id",
        "side",
        "price",
        "volume",
        "buy_no",
        "sell_no",
    ]

    assert df["symbol"].unique().tolist() == ["600000"]
    assert df["ts"].tolist() == sorted(df["ts"].tolist())
