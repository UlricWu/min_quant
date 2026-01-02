#!filepath: src/utils/csv7z_batch_source.py
from __future__ import annotations

from pathlib import Path
from typing import Iterator

import subprocess
import pyarrow as pa
import pyarrow.csv as csv
"""
Low-level I/O utility.
Not a pipeline source. Not an engine.
"""


class _Csv7zReader:
    """
    内部 wrapper（冻结）

    职责：
      - 包装 CSVStreamingReader
      - 持有 7z 子进程
      - 负责资源释放
    """

    def __init__(self, reader: csv.CSVStreamingReader, proc: subprocess.Popen):
        self._reader = reader
        self._proc = proc

    def __iter__(self):
        return iter(self._reader)

    def close(self):
        try:
            if self._proc and self._proc.poll() is None:
                self._proc.kill()
        finally:
            self._proc = None
            self._reader = None


class Csv7zBatchSource:
    """
    Csv7zBatchSource（冻结版 / Source-level Batch Provider）
    """

    # --------------------------------------------------
    def __init__(self, zfile: Path):
        if not zfile.exists():
            raise FileNotFoundError(zfile)
        if zfile.suffix != ".7z":
            raise ValueError(f"[Csv7zBatchSource] expect .7z file, got {zfile}")

        self._zfile = zfile

    # --------------------------------------------------
    def __iter__(self) -> Iterator[pa.RecordBatch]:
        wrapper = self._open_reader()
        try:
            for record_batch in wrapper:
                yield record_batch
        finally:
            wrapper.close()

    # --------------------------------------------------
    @staticmethod
    def _read_header(zfile: Path) -> list[str]:
        proc = subprocess.Popen(
            ["7z", "x", "-so", str(zfile)],
            stdout=subprocess.PIPE,
        )

        try:
            header = proc.stdout.readline()
            if not header:
                raise RuntimeError(f"[Csv7zBatchSource] empty header: {zfile}")
            return header.decode("utf-8").strip().split(",")
        finally:
            proc.kill()

    # --------------------------------------------------
    def _open_reader(self) -> _Csv7zReader:
        column_names = self._read_header(self._zfile)

        proc = subprocess.Popen(
            ["7z", "x", "-so", str(self._zfile)],
            stdout=subprocess.PIPE,
        )

        convert_opts = csv.ConvertOptions(
            column_types={name: pa.string() for name in column_names},
            strings_can_be_null=True,
            null_values=["", " ", "NULL", "N/A", "nan"],
            quoted_strings_can_be_null=True,
        )

        read_opts = csv.ReadOptions(
            autogenerate_column_names=False,
            skip_rows=1,
            column_names=column_names,
            block_size=1 << 27,  # 128MB
            use_threads=True,
        )

        reader = csv.open_csv(
            proc.stdout,
            read_options=read_opts,
            convert_options=convert_opts,
        )

        return _Csv7zReader(reader, proc)
