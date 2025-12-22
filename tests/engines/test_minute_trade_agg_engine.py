from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.engines.minute_trade_agg_engine import (
    MinuteTradeAggEngine,
    NS_PER_MINUTE,
)
from src.pipeline.context import EngineContext


def _write_parquet(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def _read_table(path: Path) -> pa.Table:
    return pq.read_table(path)


# ---------------------------------------------------------
# 基础正确性：OHLC + sum + count
# ---------------------------------------------------------
def test_minute_trade_agg_basic(tmp_path: Path) -> None:
    """
    单一 symbol，跨两个 minute，验证：
    - minute bucket
    - open / high / low / close
    - volume / signed_volume / notional
    - trade_count
    """
    base_ts = 1_000_000_000_000_000_000  # arbitrary ns

    table = pa.table(
        {
            "ts": [
                base_ts + 1,
                base_ts + 10,
                base_ts + NS_PER_MINUTE + 5,
            ],
            "price": [10.0, 12.0, 11.0],
            "volume": [100, 200, 300],
            "signed_volume": [100, 200, -300],
            "notional": [1000.0, 2400.0, 3300.0],
        }
    )

    in_path = tmp_path / "trade_enriched.parquet"
    out_path = tmp_path / "minute_trade.parquet"

    _write_parquet(in_path, table)

    engine = MinuteTradeAggEngine()
    ctx = EngineContext(
        mode="offline",
        input_path=in_path,
        output_path=out_path,
    )
    engine.execute(ctx)

    result = _read_table(out_path)
    assert result.num_rows == 2

    data = result.to_pydict()

    # minute 0
    assert data["price_first"][0] == 10.0  # open
    assert data["price_max"][0] == 12.0
    assert data["price_min"][0] == 10.0
    assert data["price_last"][0] == 12.0
    assert data["volume_sum"][0] == 300
    assert data["signed_volume_sum"][0] == 300
    assert data["notional_sum"][0] == 3400.0
    assert data["price_count"][0] == 2

    # minute 1
    assert data["price_first"][1] == 11.0
    assert data["price_last"][1] == 11.0
    assert data["volume_sum"][1] == 300
    assert data["signed_volume_sum"][1] == -300
    assert data["notional_sum"][1] == 3300.0
    assert data["price_count"][1] == 1


# ---------------------------------------------------------
# 边界情况：空输入
# ---------------------------------------------------------
def test_minute_trade_agg_empty_input(tmp_path: Path) -> None:
    table = pa.table(
        {
            "ts": pa.array([], pa.int64()),
            "price": pa.array([], pa.float64()),
            "volume": pa.array([], pa.int64()),
            "signed_volume": pa.array([], pa.int64()),
            "notional": pa.array([], pa.float64()),
        }
    )

    in_path = tmp_path / "trade_enriched.parquet"
    out_path = tmp_path / "minute_trade.parquet"

    _write_parquet(in_path, table)

    engine = MinuteTradeAggEngine()
    ctx = EngineContext(
        mode="offline",
        input_path=in_path,
        output_path=out_path,
    )
    engine.execute(ctx)

    result = _read_table(out_path)
    assert result.num_rows == 0
