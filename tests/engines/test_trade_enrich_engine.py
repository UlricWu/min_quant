from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.pipeline.context import EngineContext


# ------------------------------------------------------------
# helper: 写原始 trade parquet
# ------------------------------------------------------------
def write_raw_trades(path, rows):
    schema = pa.schema(
        [
            ("ts", pa.int64()),
            ("price", pa.float64()),
            ("volume", pa.int64()),
            ("side", pa.string()),   # B / S
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


# ============================================================
# 1. 单笔买成交
# ============================================================
def test_trade_enrich_buy(tmp_path):
    in_path = tmp_path / "trade.parquet"
    out_path = tmp_path / "trade_enriched.parquet"

    rows = [
        {
            "ts": 1,
            "price": 10.0,
            "volume": 100,
            "side": "B",
        }
    ]

    write_raw_trades(in_path, rows)

    engine = TradeEnrichEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()
    assert len(df) == 1

    r = df.iloc[0]

    assert r["volume"] == 100
    assert r["signed_volume"] == 100
    assert r["notional"] == 1000.0
    assert r["side"] == "B"


# ============================================================
# 2. 单笔卖成交
# ============================================================
def test_trade_enrich_sell(tmp_path):
    in_path = tmp_path / "trade.parquet"
    out_path = tmp_path / "trade_enriched.parquet"

    rows = [
        {
            "ts": 1,
            "price": 20.0,
            "volume": 50,
            "side": "S",
        }
    ]

    write_raw_trades(in_path, rows)

    engine = TradeEnrichEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    r = pq.read_table(out_path).to_pandas().iloc[0]

    assert r["signed_volume"] == -50
    assert r["notional"] == 1000.0


# ============================================================
# 3. 买卖混合（行数不变）
# ============================================================
def test_trade_enrich_mixed(tmp_path):
    in_path = tmp_path / "trade.parquet"
    out_path = tmp_path / "trade_enriched.parquet"

    rows = [
        {"ts": 1, "price": 10.0, "volume": 100, "side": "B"},
        {"ts": 2, "price": 11.0, "volume": 200, "side": "S"},
        {"ts": 3, "price": 12.0, "volume": 300, "side": "B"},
    ]

    write_raw_trades(in_path, rows)

    engine = TradeEnrichEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()

    assert len(df) == 3

    assert df.iloc[0]["signed_volume"] == 100
    assert df.iloc[1]["signed_volume"] == -200
    assert df.iloc[2]["signed_volume"] == 300

    assert df["notional"].tolist() == [
        1000.0,
        2200.0,
        3600.0,
    ]


# ============================================================
# 4. 不允许 NULL（分钟聚合前提）
# ============================================================
def test_trade_enrich_no_nulls(tmp_path):
    in_path = tmp_path / "trade.parquet"
    out_path = tmp_path / "trade_enriched.parquet"

    rows = [
        {"ts": 1, "price": 10.0, "volume": 100, "side": "B"},
    ]

    write_raw_trades(in_path, rows)

    engine = TradeEnrichEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    table = pq.read_table(out_path)

    for col in ["signed_volume", "notional"]:
        assert table.column(col).null_count == 0


# ============================================================
# 5. 输出 schema 固定（防回归）
# ============================================================
def test_trade_enrich_schema(tmp_path):
    in_path = tmp_path / "trade.parquet"
    out_path = tmp_path / "trade_enriched.parquet"

    rows = [
        {"ts": 1, "price": 1.0, "volume": 1, "side": "B"},
    ]

    write_raw_trades(in_path, rows)

    engine = TradeEnrichEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    table = pq.read_table(out_path)

    assert table.schema.names == ['ts', 'price', 'volume', 'side', 'notional', 'signed_volume']
