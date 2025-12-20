# tests/normalize/test_outputs_existence.py
from src.engines.normalize_engine import NormalizeEngine
from src.engines.context import EngineContext


def test_outputs_existence(parquet_input_dir, canonical_output_dir, date):
    engine = NormalizeEngine()
    engine.execute(
        EngineContext(
            mode="offline",
            date=date,
            input_path=parquet_input_dir,
            output_path=canonical_output_dir,
        )
    )

    files = {p.name for p in canonical_output_dir.glob("*.parquet")}

    # SH Trade 一定存在（我们构造了合法数据）
    assert "SH_Trade.parquet" in files

    # SZ Trade / Order：存在或不存在都允许（取决于过滤结果）
# tests/normalize/test_trade_behavior.py
import pandas as pd
from pathlib import Path


# tests/normalize/test_order_behavior.py
import pandas as pd
from pathlib import Path


def test_order_behavior(canonical_output_dir: Path):
    for path in canonical_output_dir.glob("*_Order.parquet"):
        df = pd.read_parquet(path)
        assert len(df) > 0
        assert df["event"].isin({"ADD", "CANCEL"}).all()

# tests/normalize/test_schema_lock.py
import pandas as pd

CANONICAL_COLS = [
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


def test_schema_lock(canonical_output_dir):
    for path in canonical_output_dir.glob("*.parquet"):
        df = pd.read_parquet(path)
        assert list(df.columns) == CANONICAL_COLS
        assert pd.api.types.is_integer_dtype(df["ts"])
# tests/normalize/test_symbol_hygiene.py
import pandas as pd


def test_symbol_hygiene(canonical_output_dir):
    for path in canonical_output_dir.glob("*.parquet"):
        df = pd.read_parquet(path)
        for s in df["symbol"]:
            assert len(s) == 6
            assert s.isdigit()
            assert not s.startswith("200")
