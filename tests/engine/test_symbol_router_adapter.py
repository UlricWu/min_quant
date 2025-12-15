from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import pytest

from src.engines.symbol_router_engine import SymbolRouterEngine
from src.adapters.symbol_router_adapter import SymbolRouterAdapter
from src.observability.instrumentation import Instrumentation


@pytest.fixture
def sample_parquet(tmp_path: Path) -> Path:
    df = pd.DataFrame(
        {
            "SecurityID": ["600000", "600000", "000001"],
            "Price": [10.0, 10.1, 9.8],
            "Volume": [100, 200, 300],
        }
    )
    table = pa.Table.from_pandas(df)
    path = tmp_path / "SH_Trade.parquet"
    pq.write_table(table, path)
    return path


def test_symbol_router_engine_and_adapter(tmp_path: Path, sample_parquet: Path):
    inst = Instrumentation(enabled=True)

    engine = SymbolRouterEngine()
    adapter = SymbolRouterAdapter(
        engine=engine,
        symbols=["600000"],
        inst=inst,
    )

    symbol_dir = tmp_path / "symbol"

    adapter.split(
        date="2025-11-04",
        parquet_files=[sample_parquet],
        symbol_dir=symbol_dir,
    )

    out = symbol_dir / "600000" / "2025-11-04" / "Trade.parquet"
    assert out.exists()

    table = pq.read_table(out)
    df = table.to_pandas()

    assert (df["SecurityID"] == "600000").all()
    assert len(df) == 2

    # timeline 只应包含 leaf
    assert "symbol_router_route_Trade" in inst.timeline
