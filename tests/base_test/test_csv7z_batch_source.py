# tests/utils/test_csv7z_batch_source.py
from __future__ import annotations

import io
from pathlib import Path
from typing import Any, Iterator, Optional

import pyarrow as pa
import pyarrow.csv as csv
import pytest

from src.utils.csv7z_batch_source import Csv7zBatchSource


# =============================================================================
# Fakes
# =============================================================================
class FakeProc:
    """
    伪造 subprocess.Popen 返回对象：
      - stdout: BytesIO
      - kill(): 记录调用
      - poll(): 模拟进程存活/退出
    """

    def __init__(self, *, header_line: bytes = b"", alive: bool = True):
        self.stdout = io.BytesIO(header_line)
        self._alive = alive
        self.kill_called = 0
        self.args: Optional[list[str]] = None

    def kill(self) -> None:
        self.kill_called += 1
        self._alive = False

    def poll(self) -> Optional[int]:
        # None 表示 still running
        return None if self._alive else 0


class FakeCSVStreamingReader:
    """
    伪造 pyarrow.csv.CSVStreamingReader：
      - 可迭代，yield RecordBatch
      - 可选择在某次迭代抛异常，模拟 parse 失败/中断
    """

    def __init__(self, batches: list[pa.RecordBatch], *, raise_after: Optional[int] = None):
        self._batches = batches
        self._raise_after = raise_after

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        for i, b in enumerate(self._batches):
            if self._raise_after is not None and i >= self._raise_after:
                raise RuntimeError("boom from FakeCSVStreamingReader")
            yield b


# =============================================================================
# Helpers
# =============================================================================
def make_rb(n: int = 3) -> pa.RecordBatch:
    return pa.record_batch(
        [
            pa.array(["A"] * n),
            pa.array([1] * n, type=pa.int64()),
        ],
        names=["SecurityID", "TickTime"],
    )


# =============================================================================
# Tests: init validation
# =============================================================================
def test_init_missing_file_raises(tmp_path: Path):
    missing = tmp_path / "nope.csv.7z"
    with pytest.raises(FileNotFoundError):
        Csv7zBatchSource(missing)


def test_init_wrong_suffix_raises(tmp_path: Path):
    p = tmp_path / "x.zip"
    p.write_bytes(b"dummy")
    with pytest.raises(ValueError):
        Csv7zBatchSource(p)


# =============================================================================
# Tests: _read_header contract
# =============================================================================
def test_read_header_reads_one_line_and_kills_proc(monkeypatch, tmp_path: Path):
    zfile = tmp_path / "SZ_Trade.csv.7z"
    zfile.write_bytes(b"dummy")

    # 让 Popen 第一次调用（header）返回带 header_line 的 proc
    header_line = b"SecurityID,TickTime,Price\n"
    header_proc = FakeProc(header_line=header_line, alive=True)

    popen_calls: list[list[str]] = []

    def fake_popen(args: list[str], stdout: Any = None):
        popen_calls.append(args)
        return header_proc

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    cols = Csv7zBatchSource._read_header(zfile)

    assert cols == ["SecurityID", "TickTime", "Price"]
    assert header_proc.kill_called == 1

    # 确保调用参数是 streaming 7z 输出到 stdout
    assert popen_calls[0][:3] == ["7z", "x", "-so"]


def test_read_header_empty_raises(monkeypatch, tmp_path: Path):
    zfile = tmp_path / "SZ_Trade.csv.7z"
    zfile.write_bytes(b"dummy")

    header_proc = FakeProc(header_line=b"", alive=True)

    def fake_popen(args: list[str], stdout: Any = None):
        return header_proc

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    with pytest.raises(RuntimeError):
        Csv7zBatchSource._read_header(zfile)

    assert header_proc.kill_called == 1


# =============================================================================
# Tests: _open_reader parameters
# =============================================================================
def test_open_reader_passes_expected_csv_options(monkeypatch, tmp_path: Path):
    zfile = tmp_path / "SZ_Trade.csv.7z"
    zfile.write_bytes(b"dummy")

    # Popen 会被调用两次：
    #  1) _read_header
    #  2) 真正 streaming
    header_line = b"SecurityID,TickTime,Price\n"
    header_proc = FakeProc(header_line=header_line, alive=True)
    stream_proc = FakeProc(header_line=b"", alive=True)

    popen_n = {"n": 0}

    def fake_popen(args: list[str], stdout: Any = None):
        popen_n["n"] += 1
        return header_proc if popen_n["n"] == 1 else stream_proc

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    captured: dict[str, Any] = {}

    def fake_open_csv(binary_stream, read_options=None, convert_options=None):
        captured["binary_stream"] = binary_stream
        captured["read_options"] = read_options
        captured["convert_options"] = convert_options
        # 返回可迭代 reader
        return FakeCSVStreamingReader([make_rb(2)])

    monkeypatch.setattr(csv, "open_csv", fake_open_csv)

    src = Csv7zBatchSource(zfile)
    wrapper = src._open_reader()  # 内部返回 _Csv7zReader

    # 验证 open_csv 的输入 stream 是 proc.stdout
    assert captured["binary_stream"] is stream_proc.stdout

    ro = captured["read_options"]
    co = captured["convert_options"]

    assert isinstance(ro, csv.ReadOptions)
    assert isinstance(co, csv.ConvertOptions)

    # 关键冻结参数
    assert ro.skip_rows == 1
    assert ro.autogenerate_column_names is False
    assert ro.column_names == ["SecurityID", "TickTime", "Price"]
    assert ro.block_size == (1 << 27)

    # ConvertOptions：强制所有列 string
    assert set(co.column_types.keys()) == {"SecurityID", "TickTime", "Price"}
    for t in co.column_types.values():
        assert pa.types.is_string(t)

    # 清理资源
    wrapper.close()
    assert stream_proc.kill_called == 1


# =============================================================================
# Tests: iteration + resource cleanup
# =============================================================================
def test_iter_yields_batches_and_closes_proc(monkeypatch, tmp_path: Path):
    zfile = tmp_path / "SZ_Trade.csv.7z"
    zfile.write_bytes(b"dummy")

    header_line = b"SecurityID,TickTime\n"
    header_proc = FakeProc(header_line=header_line, alive=True)
    stream_proc = FakeProc(header_line=b"", alive=True)

    popen_n = {"n": 0}

    def fake_popen(args: list[str], stdout: Any = None):
        popen_n["n"] += 1
        return header_proc if popen_n["n"] == 1 else stream_proc

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    batches = [make_rb(1), make_rb(2), make_rb(3)]

    def fake_open_csv(binary_stream, read_options=None, convert_options=None):
        return FakeCSVStreamingReader(batches)

    monkeypatch.setattr(csv, "open_csv", fake_open_csv)

    src = Csv7zBatchSource(zfile)
    got = list(iter(src))

    assert len(got) == 3
    assert got[0].num_rows == 1
    assert got[1].num_rows == 2
    assert got[2].num_rows == 3

    # __iter__ finally 必须 close -> kill proc
    assert stream_proc.kill_called == 1


def test_iter_closes_proc_on_reader_exception(monkeypatch, tmp_path: Path):
    zfile = tmp_path / "SZ_Trade.csv.7z"
    zfile.write_bytes(b"dummy")

    header_line = b"SecurityID,TickTime\n"
    header_proc = FakeProc(header_line=header_line, alive=True)
    stream_proc = FakeProc(header_line=b"", alive=True)

    popen_n = {"n": 0}

    def fake_popen(args: list[str], stdout: Any = None):
        popen_n["n"] += 1
        return header_proc if popen_n["n"] == 1 else stream_proc

    monkeypatch.setattr("subprocess.Popen", fake_popen)

    # 读到第 1 个 batch 后抛异常
    batches = [make_rb(1), make_rb(2)]

    def fake_open_csv(binary_stream, read_options=None, convert_options=None):
        return FakeCSVStreamingReader(batches, raise_after=1)

    monkeypatch.setattr(csv, "open_csv", fake_open_csv)

    src = Csv7zBatchSource(zfile)

    it = iter(src)
    first = next(it)
    assert first.num_rows == 1

    with pytest.raises(RuntimeError):
        next(it)

    # 即使异常，也必须 close -> kill proc
    assert stream_proc.kill_called == 1
