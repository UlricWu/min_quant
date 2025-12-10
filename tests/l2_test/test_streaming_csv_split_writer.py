#!filepath: tests/dataloader/streaming_csv_split_writer/test_streaming_csv_split_writer.py

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.dataloader.streaming_csv_split_writer.extractor import CsvExtractor
from src.dataloader.streaming_csv_split_writer.parser import CsvBatchParser
from src.dataloader.streaming_csv_split_writer.filters import TickTypeSplitter
from src.dataloader.streaming_csv_split_writer.writers import (
    ParquetFileWriter,
    SplitWriter,
)
from src.dataloader.streaming_csv_split_writer.converter import StreamingCsvSplitConverter
from src.dataloader.streaming_csv_split_writer.router import FileTypeRouter


# ============================================================
# 1. 测试 CsvExtractor（mock subprocess.Popen，避免依赖系统 7z）
# ============================================================

class DummyProc:
    def __init__(self, data: bytes):
        self.stdout = io.BytesIO(data)


def test_csv_extractor_returns_stdout(monkeypatch):
    """CsvExtractor.extract 应该返回 Popen.stdout"""

    dummy_data = b"dummy csv data\n"

    def fake_popen(cmd, stdout):
        # 简单验证命令中包含 7z
        assert "7z" in cmd[0].lower()
        return DummyProc(dummy_data)

    from src.dataloader.streaming_csv_split_writer import extractor as extractor_mod

    monkeypatch.setattr(extractor_mod.subprocess, "Popen", fake_popen)

    ext = CsvExtractor()
    out = ext.extract(Path("dummy.7z"))

    # 返回的应该是一个可读取的类似 file-like 对象
    assert hasattr(out, "read")
    assert out.read() == dummy_data


# ============================================================
# 2. 测试 CsvBatchParser（open_reader + cast_to_string_batch）
# ============================================================

def test_csv_batch_parser_reads_and_casts_to_string():
    """CsvBatchParser 应该能读取简单 CSV 并将列转换为 string"""

    csv_bytes = b"col1,col2\n1,2\n3,4\n"
    byte_stream = io.BytesIO(csv_bytes)

    parser = CsvBatchParser()
    reader = parser.open_reader(byte_stream)

    batches = list(reader)
    assert len(batches) == 1
    batch = batches[0]

    # 先检查原始类型（可能不是 string）
    assert batch.num_rows == 2
    assert batch.num_columns == 2

    str_batch = parser.cast_to_string_batch(batch)

    # 所有列都应该是 string 类型
    for arr in str_batch.columns:
        assert pa.types.is_string(arr.type)

    # 数据内容保持一致
    col1 = str_batch.column(0).to_pylist()
    col2 = str_batch.column(1).to_pylist()
    assert col1 == ["1", "3"]
    assert col2 == ["2", "4"]


# ============================================================
# 3. 测试 TickTypeSplitter（拆分 Order / Trade）
# ============================================================

def test_tick_type_splitter_splits_order_and_trade():
    """TickTypeSplitter 应正确根据 TickType 拆分"""

    splitter = TickTypeSplitter()

    tick_values = ["A", "D", "M", "T", "X", None]
    other_values = [10, 20, 30, 40, 50, 60]

    batch = pa.RecordBatch.from_arrays(
        [
            pa.array(tick_values, type=pa.string()),
            pa.array(other_values, type=pa.int32()),
        ],
        names=[splitter.TICK_COL, "Value"],
    )

    order_batch, trade_batch = splitter.split(batch)

    # A/D/M 属于 Order
    order_tick = order_batch.column(0).to_pylist()
    assert set(order_tick) == {"A", "D", "M"}

    # T 属于 Trade
    trade_tick = trade_batch.column(0).to_pylist()
    assert set(trade_tick) == {"T"}


# ============================================================
# 4. 测试 ParquetFileWriter / SplitWriter
# ============================================================

def test_parquet_file_writer_writes_and_reads_back(tmp_path):
    """ParquetFileWriter 应该能写多个 batch 并正确读回"""

    out_path = tmp_path / "test.parquet"
    writer = ParquetFileWriter(out_path)

    batch1 = pa.RecordBatch.from_arrays(
        [pa.array(["a", "b"], type=pa.string())],
        names=["col"],
    )
    batch2 = pa.RecordBatch.from_arrays(
        [pa.array(["c"], type=pa.string())],
        names=["col"],
    )

    writer.write(batch1)
    writer.write(batch2)
    writer.close()

    assert out_path.exists()

    table = pq.read_table(out_path)
    assert table.num_rows == 3
    assert table.column("col").to_pylist() == ["a", "b", "c"]


def test_split_writer_writes_order_and_trade(tmp_path):
    """SplitWriter 应该能分别写 Order 与 Trade 文件"""

    order_path = tmp_path / "order.parquet"
    trade_path = tmp_path / "trade.parquet"

    split_writer = SplitWriter(order_path, trade_path)

    order_batch = pa.RecordBatch.from_arrays(
        [pa.array(["A", "D"], type=pa.string())],
        names=["TickType"],
    )
    trade_batch = pa.RecordBatch.from_arrays(
        [pa.array(["T"], type=pa.string())],
        names=["TickType"],
    )

    split_writer.write_order(order_batch)
    split_writer.write_trade(trade_batch)
    split_writer.close()

    assert order_path.exists()
    assert trade_path.exists()

    order_table = pq.read_table(order_path)
    trade_table = pq.read_table(trade_path)

    assert order_table.num_rows == 2
    assert trade_table.num_rows == 1
    assert set(order_table.column("TickType").to_pylist()) == {"A", "D"}
    assert trade_table.column("TickType").to_pylist() == ["T"]


# ============================================================
# 5. 测试 FileTypeRouter（基础行为）
# ============================================================

def test_file_type_router_basic(tmp_path):
    from src.dataloader.streaming_csv_split_writer.router import FileTypeRouter

    router = FileTypeRouter()

    # SH 混合
    routes = router.route("SH_MIXED", tmp_path)
    assert routes.split is True
    assert routes.single_output is None

    # 单文件
    routes2 = router.route("SZ_ORDER", tmp_path)
    assert routes2.split is False
    assert str(routes2.single_output).endswith("SZ_Order.parquet")

    with pytest.raises(RuntimeError):
        router.route("UNKNOWN_TYPE", tmp_path)


# ============================================================
# 6. 测试 StreamingCsvSplitConverter（核心整合测试）
#    - 使用 monkeypatch 替代真实 7z 解压 & CSV 解析（避免外部依赖）
# ============================================================

def _make_dummy_batches_for_sh_mixed():
    """构造两个 batch：含 TickType A/T/M 等，用于 SH_MIXED 测试"""
    splitter = TickTypeSplitter()

    batch1 = pa.RecordBatch.from_arrays(
        [
            pa.array(["A", "T", "M"], type=pa.string()),
            pa.array([1, 2, 3], type=pa.int32()),
        ],
        names=[splitter.TICK_COL, "Value"],
    )

    batch2 = pa.RecordBatch.from_arrays(
        [
            pa.array(["D", "T"], type=pa.string()),
            pa.array([4, 5], type=pa.int32()),
        ],
        names=[splitter.TICK_COL, "Value"],
    )
    return [batch1, batch2]


def _make_dummy_batches_for_single():
    """构造两个 batch：不关心 TickType，仅测试单文件写出"""
    batch1 = pa.RecordBatch.from_arrays(
        [pa.array(["x", "y"], type=pa.string())],
        names=["Col"],
    )
    batch2 = pa.RecordBatch.from_arrays(
        [pa.array(["z"], type=pa.string())],
        names=["Col"],
    )
    return [batch1, batch2]


def test_converter_sh_mixed_streaming(monkeypatch, tmp_path):
    """
    StreamingCsvSplitConverter 在 SH_MIXED 模式下应：
    - 创建 SH_Order.parquet 与 SH_Trade.parquet
    - Order 内只包含 A/D/M，Trade 内只包含 T
    """

    dummy_batches = _make_dummy_batches_for_sh_mixed()

    conv = StreamingCsvSplitConverter()

    # 1) mock extractor.extract：不真实解压，只返回占位对象
    class DummyExtractor:
        def extract(self, zfile: Path):
            return io.BytesIO(b"dummy")

    # 2) mock parser.open_reader：不解析 CSV，直接返回 dummy_batches
    class DummyParser:
        def open_reader(self, byte_stream):
            # 忽略 byte_stream，直接返回一个可迭代的 batch 列表
            return iter(dummy_batches)

        def cast_to_string_batch(self, batch):
            # 假设已经是 string，无需转换
            return batch

    monkeypatch.setattr(conv, "extractor", DummyExtractor())
    monkeypatch.setattr(conv, "parser", DummyParser())

    zfile = tmp_path / "SH_Stock_OrderTrade.csv.7z"
    zfile.write_bytes(b"not_used")

    conv.convert(zfile, tmp_path, file_type="SH_MIXED")

    order_path = tmp_path / "SH_Order.parquet"
    trade_path = tmp_path / "SH_Trade.parquet"

    assert order_path.exists()
    assert trade_path.exists()

    order_table = pq.read_table(order_path)
    trade_table = pq.read_table(trade_path)

    order_ticks = order_table.column("TickType").to_pylist()
    trade_ticks = trade_table.column("TickType").to_pylist()

    assert set(order_ticks) == {"A", "M", "D"}
    assert set(trade_ticks) == {"T"}


def test_converter_single_output(monkeypatch, tmp_path):
    """
    StreamingCsvSplitConverter 在单文件模式（如 SZ_ORDER）下应：
    - 只创建一个 parquet
    - 行数为所有 batch 总和
    """

    dummy_batches = _make_dummy_batches_for_single()

    conv = StreamingCsvSplitConverter()

    class DummyExtractor:
        def extract(self, zfile: Path):
            return io.BytesIO(b"dummy")

    class DummyParser:
        def open_reader(self, byte_stream):
            return iter(dummy_batches)

        def cast_to_string_batch(self, batch):
            return batch

    monkeypatch.setattr(conv, "extractor", DummyExtractor())
    monkeypatch.setattr(conv, "parser", DummyParser())

    zfile = tmp_path / "SZ_Order.csv.7z"
    zfile.write_bytes(b"not_used")

    conv.convert(zfile, tmp_path, file_type="SZ_ORDER")

    out_path = tmp_path / "SZ_Order.parquet"
    assert out_path.exists()

    table = pq.read_table(out_path)
    assert table.num_rows == 3
    assert table.column("Col").to_pylist() == ["x", "y", "z"]
