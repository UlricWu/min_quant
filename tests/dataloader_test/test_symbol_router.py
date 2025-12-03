#!filepath: tests/dataloader_test/test_symbol_router.py

from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from src.dataloader.symbol_router import SymbolRouter


# ------------------------------------------------------------
# 工具函数：构造一个简单的 Arrow Table
# ------------------------------------------------------------
def make_mock_table(symbol_rows: dict) -> pa.Table:
    """
    symbol_rows: dict, key 为 symbol(int 或 str)，value 为该 symbol 行数
    例如: {"603322": 3, "2594": 2}
    """
    security_ids = []
    values = []
    for sid, n in symbol_rows.items():
        sid_int = int(sid)
        for i in range(n):
            security_ids.append(sid_int)
            values.append(i)

    return pa.table(
        {
            "SecurityID": pa.array(security_ids, type=pa.int32()),
            "value": pa.array(values, type=pa.int32()),
        }
    )


# ------------------------------------------------------------
# fixture：构造临时目录结构
# ------------------------------------------------------------
@pytest.fixture
def mock_paths(tmp_path):
    """
    构造如下结构（全部在 tmp_path 下）：

    tmp_path/
      parquet/
        2025-01-03/
          SH_Order.parquet
          SZ_Order.parquet
      symbol/
        （后续按 symbol / date 创建）
    """
    parquet_root = tmp_path / "parquet"
    parquet_root.mkdir()

    symbol_root = tmp_path / "symbol"
    symbol_root.mkdir()

    date_dir = parquet_root / "2025-01-03"
    date_dir.mkdir()

    sh_order = date_dir / "SH_Order.parquet"
    sz_order = date_dir / "SZ_Order.parquet"

    sh_order.touch()
    sz_order.touch()

    return {
        "parquet_root": parquet_root,
        "symbol_root": symbol_root,
        "date_dir": date_dir,
        "sh_order": sh_order,
        "sz_order": sz_order,
    }


# ------------------------------------------------------------
# fixture：mock PathManager
# ------------------------------------------------------------
@pytest.fixture
def patch_path_manager(mock_paths):
    """
    mock:
      - PathManager.parquet_dir() → mock_paths["parquet_root"]
      - PathManager.symbol_dir(symbol, date) → symbol_root / symbol / date
    """
    with patch("src.dataloader.symbol_router.PathManager") as PM:
        PM.parquet_dir.return_value = mock_paths["parquet_root"]
        PM.symbol_dir.side_effect = (
            lambda symbol, date: mock_paths["symbol_root"] / str(symbol) / date
        )
        yield PM


# ------------------------------------------------------------
# fixture：mock RouterMetadata，避免真实写入
# ------------------------------------------------------------
@pytest.fixture
def patch_metadata():
    with patch("src.dataloader.symbol_router.RouterMetadata") as RM:
        meta = RM.return_value
        meta.reset.return_value = None
        meta.save.return_value = None
        meta.add_symbol_output.return_value = None
        meta.add_filtered.return_value = None
        yield RM


# ------------------------------------------------------------
# fixture：mock FileSystem.ensure_dir，保证兼容
# ------------------------------------------------------------
@pytest.fixture
def patch_filesystem():
    def _ensure_dir(path: Path):
        path.mkdir(parents=True, exist_ok=True)

    with patch("src.dataloader.symbol_router.FileSystem") as FS:
        FS.ensure_dir.side_effect = _ensure_dir
        yield FS


# ------------------------------------------------------------
# 测试 1：混合交易所 + smart-skip
# ------------------------------------------------------------
def test_symbol_router_smart_skip_mixed_exchange(
    mock_paths, patch_path_manager, patch_metadata, patch_filesystem
):
    """
    场景：

    - self.symbols = ["603322", "002594"] （上交所 + 深交所混合）
    - 有两个 parquet:
        SH_Order.parquet
        SZ_Order.parquet
    - 预先创建 SZ 002594 的目标文件，使得 SZ_Order.parquet 可被完全跳过

    期望：

    1. SH_Order.parquet 被读取（因为 603322 文件不存在，需要拆分）
    2. SZ_Order.parquet 不被读取（因为 002594 对应文件已存在 → smart-skip）
    3. 只写出 603322 对应文件，不写 002594
    """

    # 这里用字符串 symbol，兼容你的 __init__（无论是 int → str 还是直接 str）
    symbols = ["603322", "002594"]
    router = SymbolRouter(symbols=symbols)

    # mock 的 Arrow 表：603322 有 3 行，002594 有 2 行
    mock_table = make_mock_table({"603322": 3, "2594": 2})

    # 为 SZ 002594 创建已存在的目标文件：symbol_dir("002594", "2025-01-03") / "Order.parquet"
    sz_symbol_dir = mock_paths["symbol_root"] / "002594" / "2025-01-03"
    sz_symbol_dir.mkdir(parents=True, exist_ok=True)
    (sz_symbol_dir / "Order.parquet").touch()

    # patch pq.read_table / pq.write_table（注意 patch 别名 pq）
    with patch(
        "src.dataloader.symbol_router.pq.read_table", return_value=mock_table
    ) as mock_read, patch(
        "src.dataloader.symbol_router.pq.write_table"
    ) as mock_write:

        router.route_date("2025-01-03")

        # ---- 校验 read_table 调用情况 ----
        # 应该至少被调用一次（SH_Order.parquet）
        assert mock_read.call_count >= 1

        called_files = [str(call.args[0]) for call in mock_read.call_args_list]

        # SH_Order.parquet 肯定要被读
        assert any(
            "SH_Order.parquet" in f for f in called_files
        ), "SH_Order.parquet 应被读取"

        # SZ_Order.parquet 不应被读（smart-skip 生效）
        assert not any(
            "SZ_Order.parquet" in f for f in called_files
        ), "SZ_Order.parquet 应被 smart-skip，不能被读取"

        # ---- 校验 write_table 调用情况 ----
        assert mock_write.call_count >= 1, "应至少写出一个 symbol 文件"

        write_paths = [str(call.args[1]) for call in mock_write.call_args_list]

        # 应写出 603322
        assert any(
            "603322" in p for p in write_paths
        ), "应包含 SH symbol=603322 的输出文件"

        # 不应写出 002594（因为已经存在）
        assert not any(
            "002594" in p for p in write_paths
        ), "002594 对应文件已存在，应被 smart-skip，不应再次写出"


# ------------------------------------------------------------
# 测试 2：所有 symbol 文件都已存在 → 整个 parquet 完全跳过
# ------------------------------------------------------------
def test_symbol_router_skip_entire_parquet_when_all_exist(
    mock_paths, patch_path_manager, patch_metadata, patch_filesystem
):
    """
    场景：

    - self.symbols = ["603322"]
    - 仅关注 SH_Order.parquet
    - 预先创建 603322 对应输出文件

    期望：

    - SH_Order.parquet 完全被跳过，不调用 read_table
    """

    symbols = ["603322"]
    router = SymbolRouter(symbols=symbols)

    # 预先创建 603322 的目标文件：symbol_dir("603322", "2025-01-03") / "Order.parquet"
    sh_symbol_dir = mock_paths["symbol_root"] / "603322" / "2025-01-03"
    sh_symbol_dir.mkdir(parents=True, exist_ok=True)
    (sh_symbol_dir / "Order.parquet").touch()

    # patch read_table，验证不被调用
    with patch("src.dataloader.symbol_router.pq.read_table") as mock_read, patch(
        "src.dataloader.symbol_router.pq.write_table"
    ) as mock_write:

        router.route_date("2025-01-03")

        # 所有相关 symbol 文件已存在 → 不应该读 parquet
        assert (
            mock_read.call_count == 0
        ), "所有 symbol 输出已存在，应 smart-skip 整个 SH_Order.parquet"

        # 也不应再写任何文件
        assert (
            mock_write.call_count == 0
        ), "所有 symbol 输出已存在，不应再写 parquet 文件"

