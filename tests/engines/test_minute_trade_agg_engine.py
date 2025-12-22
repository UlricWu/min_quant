from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine
from src.pipeline.context import EngineContext

US_PER_MINUTE = 60 * 1_000_000


# ------------------------------------------------------------
# helper: 写 Trade_Enriched.parquet
# ------------------------------------------------------------
def write_trade_enriched(path, rows):
    schema = pa.schema(
        [
            ("ts", pa.int64()),            # us
            ("price", pa.float64()),
            ("volume", pa.int64()),
            ("signed_volume", pa.int64()),
            ("notional", pa.float64()),
        ]
    )
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)


# ============================================================
# 1. 单 minute OHLC
# ============================================================
def test_minute_trade_agg_single_minute_ohlc(tmp_path):
    in_path = tmp_path / "trade_enriched.parquet"
    out_path = tmp_path / "minute_trade.parquet"

    base_minute = 10
    base_ts = base_minute * US_PER_MINUTE

    rows = [
        # open
        {
            "ts": base_ts + 1,
            "price": 10.0,
            "volume": 100,
            "signed_volume": 100,
            "notional": 1000.0,
        },
        # high
        {
            "ts": base_ts + 2,
            "price": 12.0,
            "volume": 50,
            "signed_volume": -50,
            "notional": 600.0,
        },
        # low
        {
            "ts": base_ts + 3,
            "price": 9.0,
            "volume": 30,
            "signed_volume": 30,
            "notional": 270.0,
        },
        # close
        {
            "ts": base_ts + 4,
            "price": 11.0,
            "volume": 20,
            "signed_volume": -20,
            "notional": 220.0,
        },
    ]

    write_trade_enriched(in_path, rows)

    engine = MinuteTradeAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    assert out_path.exists()

    df = pq.read_table(out_path).to_pandas()
    assert len(df) == 1

    r = df.iloc[0]

    assert r["open"] == 10.0
    assert r["high"] == 12.0
    assert r["low"] == 9.0
    assert r["close"] == 11.0

    assert r["volume"] == 200
    assert r["signed_volume"] == 60
    assert r["notional"] == 2090.0

    assert r["trade_count"] == 4

    # minute 对齐
    assert int(r["minute"].value // 1_000) == base_ts


# ============================================================
# 2. 跨 minute
# ============================================================
def test_minute_trade_agg_multi_minute(tmp_path):
    in_path = tmp_path / "trade_enriched.parquet"
    out_path = tmp_path / "minute_trade.parquet"

    rows = [
        {
            "ts": 1 * US_PER_MINUTE + 10,
            "price": 10.0,
            "volume": 10,
            "signed_volume": 10,
            "notional": 100.0,
        },
        {
            "ts": 2 * US_PER_MINUTE + 10,
            "price": 20.0,
            "volume": 20,
            "signed_volume": -20,
            "notional": 400.0,
        },
    ]

    write_trade_enriched(in_path, rows)

    engine = MinuteTradeAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()

    assert len(df) == 2

    minutes = sorted(df["minute"].astype("int64").tolist())
    assert minutes[0] == 1 * US_PER_MINUTE
    assert minutes[1] == 2 * US_PER_MINUTE


# ============================================================
# 3. 单条成交
# ============================================================
def test_minute_trade_agg_single_trade(tmp_path):
    in_path = tmp_path / "trade_enriched.parquet"
    out_path = tmp_path / "minute_trade.parquet"

    rows = [
        {
            "ts": 0,
            "price": 10.0,
            "volume": 1,
            "signed_volume": 1,
            "notional": 10.0,
        }
    ]

    write_trade_enriched(in_path, rows)

    engine = MinuteTradeAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    df = pq.read_table(out_path).to_pandas()
    r = df.iloc[0]

    assert r["open"] == 10.0
    assert r["high"] == 10.0
    assert r["low"] == 10.0
    assert r["close"] == 10.0
    assert r["trade_count"] == 1


# ============================================================
# 4. 空输入
# ============================================================
def test_minute_trade_agg_empty_input(tmp_path):
    in_path = tmp_path / "trade_enriched.parquet"
    out_path = tmp_path / "minute_trade.parquet"

    schema = pa.schema(
        [
            ("ts", pa.int64()),
            ("price", pa.float64()),
            ("volume", pa.int64()),
            ("signed_volume", pa.int64()),
            ("notional", pa.float64()),
        ]
    )

    empty_table = pa.Table.from_arrays(
        [pa.array([], type=f.type) for f in schema],
        schema=schema,
    )
    pq.write_table(empty_table, in_path)

    engine = MinuteTradeAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    assert out_path.exists()
    assert pq.read_table(out_path).num_rows == 0


# ============================================================
# 5. 输出 schema 固定（防回归）
# ============================================================
def test_minute_trade_agg_schema(tmp_path):
    in_path = tmp_path / "trade_enriched.parquet"
    out_path = tmp_path / "minute_trade.parquet"

    rows = [
        {
            "ts": 0,
            "price": 1.0,
            "volume": 1,
            "signed_volume": 1,
            "notional": 1.0,
        }
    ]

    write_trade_enriched(in_path, rows)

    engine = MinuteTradeAggEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            input_path=in_path,
            output_path=out_path,
        )
    )

    table = pq.read_table(out_path)
    assert table.schema.names == [
        "minute",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "signed_volume",
        "notional",
        "trade_count",
    ]
