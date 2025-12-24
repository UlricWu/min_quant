# conftest.py 或测试文件顶部

import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import pytest


@pytest.fixture()
def make_events_parquet(tmp_path):
    """
    返回一个函数，用于生成 parquet
    """

    def _make(path: Path, rows: dict) -> Path:
        table = pa.table(
            rows,
            schema=pa.schema(
                [
                    ("ts", pa.int64()),
                    ("event", pa.string()),
                    ("order_id", pa.int64()),
                    ("side", pa.string()),
                    ("price", pa.float64()),
                    ("volume", pa.int64()),
                ]
            ),
        )
        pq.write_table(table, path)
        return path

    return _make
