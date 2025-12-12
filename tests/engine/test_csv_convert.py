#!filepath: tests/test_csv_convert.py
import pyarrow as pa
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.engines.csv_convert_engine import CsvConvertEngine
from src.adapters.csv_convert_adapter import CsvConvertAdapter
from src.dataloader.pipeline.steps.csv_to_parquet_step import CsvConvertStep
from src.dataloader.pipeline.context import PipelineContext


# ============================
# Engine Tests
# ============================
def test_engine_cast_to_string_batch():
    eng = CsvConvertEngine()
    batch = pa.record_batch(
        [pa.array([1, 2, 3])],
        names=["A"]
    )
    out = eng.cast_to_string_batch(batch)

    assert pa.types.is_string(out.column(0).type)
    assert out.num_rows == 3


def test_engine_split_sh_mixed():
    eng = CsvConvertEngine()
    batch = pa.record_batch(
        [
            pa.array(["A", "T", "D", "T"]),   # TickType
            pa.array(["x1", "x2", "x3", "x4"])
        ],
        names=["TickType", "Val"]
    )

    order_batch, trade_batch = eng.split_sh_mixed(batch)

    # TickType in [A,D,M] → 2 行
    assert order_batch.num_rows == 2
    # TickType == T → 2 行
    assert trade_batch.num_rows == 2


# ============================
# Adapter Tests (mock I/O)
# ============================
@patch("src.adapters.csv_convert_adapter.ParquetFileWriter")
@patch("src.adapters.csv_convert_adapter.SplitWriter")
@patch("src.adapters.csv_convert_adapter.CsvBatchParser")
@patch("src.adapters.csv_convert_adapter.CsvExtractor")
def test_adapter_convert_sh_mixed(
    mock_extractor_cls,
    mock_parser_cls,
    mock_split_writer_cls,
    mock_pfw_cls,
    tmp_path,
):
    # 准备 fake batch
    batch = pa.record_batch(
        [
            pa.array(["A", "T"]),   # TickType
            pa.array(["v1", "v2"]),
        ],
        names=["TickType", "Val"],
    )

    # mock extractor → 返回 byte_stream 占位
    extractor = MagicMock()
    extractor.extract.return_value = b"csv-bytes"
    mock_extractor_cls.return_value = extractor

    # mock parser → 返回 batch list
    parser = MagicMock()
    parser.open_reader.return_value = [batch]
    mock_parser_cls.return_value = parser

    # mock writer
    writer = MagicMock()
    mock_split_writer_cls.return_value = writer

    eng = CsvConvertEngine()
    adapter = CsvConvertAdapter(eng)

    zfile = tmp_path / "SH_Stock_OrderTrade.csv.7z"
    zfile.write_bytes(b"dummy")

    out_dir = tmp_path / "out"

    adapter.convert(zfile, out_dir, "SH_MIXED")

    # 确认 SplitWriter 被创建
    mock_split_writer_cls.assert_called_once()
    # write_order + write_trade 都被调用
    assert writer.write_order.called
    assert writer.write_trade.called
    writer.close.assert_called_once()


@patch("src.adapters.csv_convert_adapter.ParquetFileWriter")
@patch("src.adapters.csv_convert_adapter.CsvBatchParser")
@patch("src.adapters.csv_convert_adapter.CsvExtractor")
def test_adapter_convert_single(
    mock_extractor_cls,
    mock_parser_cls,
    mock_pfw_cls,
    tmp_path,
):
    batch = pa.record_batch(
        [
            pa.array(["A", "A"]),  # TickType (但对单文件没影响)
            pa.array(["v1", "v2"]),
        ],
        names=["TickType", "Val"],
    )

    extractor = MagicMock()
    extractor.extract.return_value = b"csv-bytes"
    mock_extractor_cls.return_value = extractor

    parser = MagicMock()
    parser.open_reader.return_value = [batch]
    mock_parser_cls.return_value = parser

    writer = MagicMock()
    mock_pfw_cls.return_value = writer

    eng = CsvConvertEngine()
    adapter = CsvConvertAdapter(eng)

    zfile = tmp_path / "SZ_Order.csv.7z"
    zfile.write_bytes(b"dummy")

    out_dir = tmp_path / "out"

    adapter.convert(zfile, out_dir, "SZ_ORDER")

    mock_pfw_cls.assert_called_once()
    writer.write.assert_called()
    writer.close.assert_called_once()


# ============================
# Step Tests
# ============================
def test_step_invokes_adapter_and_skip(tmp_path):
    # 构造 raw_dir / parquet_dir
    raw_dir = tmp_path / "raw"
    pq_dir = tmp_path / "pq"
    raw_dir.mkdir()
    pq_dir.mkdir()

    # 1) 创建一个需要处理的文件
    f1 = raw_dir / "SH_Trade.csv.7z"
    f1.write_text("x")

    # 2) 创建一个应当被 skip 的文件：SZ_Order 对应 parquet 已存在
    f2 = raw_dir / "SZ_Order.csv.7z"
    f2.write_text("y")
    (pq_dir / "SZ_Order.parquet").write_text("exists")

    ctx = PipelineContext(
        date="2025-11-04",
        raw_dir=raw_dir,
        parquet_dir=pq_dir,
        symbol_dir=tmp_path / "symbol",
    )

    adapter = MagicMock()
    step = CsvConvertStep(adapter)

    step.run(ctx)

    # 只对 SH_Trade 文件调用一次 convert
    adapter.convert.assert_called_once()
    args, kwargs = adapter.convert.call_args
    assert args[0].name == "SH_Trade.csv.7z"
    assert args[2] == "SH_TRADE"
