#!filepath: tests/normalize/test_normalize_adapter.py
from __future__ import annotations

import pandas as pd

from src.adapters.normalize_adapter import NormalizeAdapter
from src.engines.normalize_engine import NormalizeEngine


def test_adapter_runs(tmp_path):
    engine = NormalizeEngine()
    adapter = NormalizeAdapter(engine=engine, symbols=["600000"])

    date = "2025-01-02"
    base = tmp_path / "600000" / date
    base.mkdir(parents=True)

    pd.DataFrame(
        {
            "SecurityID": ["600000"],
            "TradeTime": ["20250102"],
            "TickTime": ["093000000"],
            "TickType": ["A"],
            "SubSeq": [1],
            "Side": ["1"],
            "Price": [10.0],
            "Volume": [100],
            "BuyNo": [0],
            "SellNo": [0],
        }
    ).to_parquet(base / "Order.parquet", index=False)

    adapter.run(date=date, symbol_dir=tmp_path)

    assert (base / "order" / "Normalized.parquet").exists()
