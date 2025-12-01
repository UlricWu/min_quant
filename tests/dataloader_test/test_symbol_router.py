import pyarrow as pa
import pyarrow.parquet as pq
from unittest.mock import patch
from src.dataloader.symbol_router import SymbolRouter
from src.utils.path import PathManager


DATE = "2025-11-23"


def test_symbol_router_basic_int(tmp_path):
    PathManager.set_root(tmp_path)

    # mock PathManager 目录
    with patch("src.dataloader.symbol_router.PathManager.data_dir", return_value=tmp_path / "dataloader"):
        with patch("src.dataloader.symbol_router.PathManager.parquet_dir", return_value=tmp_path / "dataloader/parquet"):

            parquet_dir = tmp_path / "dataloader/parquet" / DATE
            parquet_dir.mkdir(parents=True)

            table = pa.table({
                "SecurityID": [600000, 1, 300750, 123456],
                "price": [10, 20, 30, 40],
            })
            pq.write_table(table, parquet_dir / "SH_test.parquet")

            # 禁用 swallow error
            with patch("src.dataloader.symbol_router.logs.catch", lambda *a, **k: (lambda f: f)):

                router = SymbolRouter()
                router.route_date(DATE)

            # 验证输出
            out_600000 = tmp_path / "dataloader/symbol/600000" / DATE / "test.parquet"
            out_000001 = tmp_path / "dataloader/symbol/000001" / DATE / "test.parquet"
            out_300750 = tmp_path / "dataloader/symbol/300750" / DATE / "test.parquet"

            assert out_600000.exists()
            assert out_000001.exists()
            assert out_300750.exists()

            t = pq.read_table(out_600000)
            assert t["SecurityID"].to_pylist() == [600000]
