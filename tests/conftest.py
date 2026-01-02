# tests/conftest.py
from __future__ import annotations

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Dict, Any

import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger
from src.pipeline.context import PipelineContext


@pytest.fixture(autouse=True)
def disable_file_logger():
    logger.remove()
    logger.add(lambda msg: None)  # or sys.stderr
    yield


# todo property-based contract（Hypothesis）
@pytest.fixture(scope="session")
def date() -> str:
    return "2025-12-01"


@pytest.fixture
def parquet_input_dir(tmp_path: Path, date: str) -> Path:
    """
    /data_handler/parquet/<date>/
        SH_Order.parquet
        SH_Trade.parquet
        SZ_Order.parquet
        SZ_Trade.parquet
    """
    base = tmp_path / "data_handler" / "parquet" / date
    base.mkdir(parents=True)

    # =========================
    # SH Order / Trade
    # =========================
    sh_rows = [
        # 合法 A 股
        dict(
            TradeTime="2025-12-01 09:15:00.050",
            ExchangeID=1,
            SecurityID="600000",
            MainSeq=10002,
            SubSeq=2,
            TickTime=91500050,
            TickType="T",
            BuyNo=200,
            SellNo=0,
            Price=10.52,
            Volume=200,
            TradeMoney=2104.0,
            Side="B",
            TradeBSFlag="B",
            MDSecurityStat=0,
            LocalTimeStamp=91500153,
        ),
        # 非 A 股（应被过滤）
        dict(
            TradeTime="2025-12-01 09:15:00.060",
            ExchangeID=1,
            SecurityID="200001",
            MainSeq=20001,
            SubSeq=1,
            TickTime=91500060,
            TickType="T",
            BuyNo=1,
            SellNo=2,
            Price=1.0,
            Volume=10,
            TradeMoney=10.0,
            Side="S",
            TradeBSFlag="S",
            MDSecurityStat=0,
            LocalTimeStamp=91500154,
        ),
    ]
    sh_df = pd.DataFrame(sh_rows)
    sh_df.to_parquet(base / "SH_Order.parquet", index=False)
    sh_df.to_parquet(base / "SH_Trade.parquet", index=False)

    # =========================
    # SZ Order
    # =========================
    sz_order = pd.DataFrame(
        [
            dict(
                TradeTime="2025-12-01 09:15:00.030",
                ExchangeID=2,
                SecurityID="002936",
                OrderTime="2025-12-01 09:15:00.030",
                Price=20.00,
                Volume=600,
                Side="S",
                OrderType="L",
                MainSeq=30001,
                SubSeq=1,
                OrderNO=900001,
                OrderStatus="A",
                LocalTimeStamp=91500145,
            )
        ]
    )
    sz_order.to_parquet(base / "SZ_Order.parquet", index=False)

    # =========================
    # SZ Trade
    # =========================
    sz_trade = pd.DataFrame(
        [
            dict(
                TradeTime="2025-12-01 09:15:00.045",
                ExchangeID=2,
                SecurityID="002936",
                TickTime=91500045,
                TradePrice=20.10,
                TradeVolume=500,
                ExecType="F",
                MainSeq=40001,
                SubSeq=1,
                BuyNo=88,
                SellNo=0,
                LocalTimeStamp=91500151,
            ),
            # 非法 ExecType（应被过滤）
            dict(
                TradeTime="2025-12-01 09:15:00.046",
                ExchangeID=2,
                SecurityID="002936",
                TickTime=91500046,
                TradePrice=20.11,
                TradeVolume=600,
                ExecType="X",
                MainSeq=40002,
                SubSeq=1,
                BuyNo=89,
                SellNo=0,
                LocalTimeStamp=91500156,
            ),
        ]
    )
    sz_trade.to_parquet(base / "SZ_Trade.parquet", index=False)

    return base


@pytest.fixture
def canonical_output_dir(tmp_path: Path, date: str) -> Path:
    out = tmp_path / "data_handler" / "canonical" / date
    out.mkdir(parents=True)
    return out


# ============================================================
# patch parse_events：只测试 Normalize 契约，不测试 parser
# ============================================================
@pytest.fixture(autouse=True)
def _patch_parse_events(monkeypatch):
    import src.engines.parser_engine as ne

    def _stub_parse_events(df: pd.DataFrame, kind: Literal["order", "trade"]):
        symbol = df["SecurityID"].astype(str).str.zfill(6)
        ts = df["LocalTimeStamp"].astype(int)
        order_id = (
                df.get("MainSeq", 0).astype(int) * 1_000_000
                + df.get("SubSeq", 0).astype(int)
        )

        if kind == "trade":
            if "ExecType" in df.columns:
                event = df["ExecType"].map({"F": "TRADE"}).fillna("UNKNOWN")
                price = df["TradePrice"].astype(float)
                volume = df["TradeVolume"].astype(int)
                buy_no = df["BuyNo"].astype(int)
                sell_no = df["SellNo"].astype(int)
                side = pd.Series([None] * len(df))
            else:
                event = pd.Series(["TRADE"] * len(df))
                price = df["Price"].astype(float)
                volume = df["Volume"].astype(int)
                buy_no = df["BuyNo"].astype(int)
                sell_no = df["SellNo"].astype(int)
                side = df["Side"]
        else:
            event = pd.Series(["ADD"] * len(df))
            price = df["Price"].astype(float)
            volume = df["Volume"].astype(int)
            buy_no = df.get("BuyNo", pd.Series([0] * len(df))).astype(int)
            sell_no = df.get("SellNo", pd.Series([0] * len(df))).astype(int)
            side = df.get("Side", pd.Series([None] * len(df)))

        return pd.DataFrame(
            {
                "symbol": symbol,
                "ts": ts,
                "event": event,
                "order_id": order_id,
                "side": side,
                "price": price,
                "volume": volume,
                "buy_no": buy_no,
                "sell_no": sell_no,
            }
        )

    monkeypatch.setattr(ne, "parse_events_arrow", _stub_parse_events, raising=True)


@pytest.fixture
def write_parquet(tmp_path):
    def _write(path, rows, schema):
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, path)
        return path

    return _write


# tests/conftest.py
import multiprocessing
import pytest


@pytest.fixture(scope="session", autouse=True)
def _set_start_method():
    multiprocessing.set_start_method("spawn", force=True)


import pytest
from pathlib import Path


@pytest.fixture
def dummy_file(tmp_path: Path) -> Path:
    p = tmp_path / "input.txt"
    p.write_text("hello world", encoding="utf-8")
    return p


@pytest.fixture
def dummy_output(tmp_path: Path) -> Path:
    p = tmp_path / "output.txt"
    p.write_text("output data_handler", encoding="utf-8")
    return p


@pytest.fixture
def make_test_pipeline_context(tmp_path: Path):
    """
    Factory fixture for PipelineContext (testing only).

    Usage:
        ctx = make_test_pipeline_context()
        ctx = make_test_pipeline_context(date="2025-01-02")

    Design principles:
    - PipelineContext remains a strong, non-optional contract
    - Tests get a minimal but complete context
    - All directory paths are real and isolated under tmp_path
    """

    def _make(date: str = "2025-01-01") -> PipelineContext:
        raw_dir = tmp_path / "raw"
        parquet_dir = tmp_path / "parquet"
        fact_dir = tmp_path / "fact"
        feature_dir = tmp_path / "feature"
        meta_dir = tmp_path / "meta"
        label_dir = tmp_path / "label"

        for d in (
                raw_dir,
                parquet_dir,
                label_dir,
                fact_dir,
                feature_dir,
                meta_dir,
        ):
            d.mkdir(parents=True, exist_ok=True)

        return PipelineContext(
            date=date,
            raw_dir=raw_dir,
            parquet_dir=parquet_dir,
            fact_dir=fact_dir,
            feature_dir=feature_dir,
            meta_dir=meta_dir,
            label_dir=label_dir
        )

    return _make
