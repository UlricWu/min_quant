# src/engines/extractor_engine.py
from __future__ import annotations
from pathlib import Path
import subprocess
import pyarrow as pa
import pyarrow.csv as csv


class ExtractorEngine:
    """
    工业级 CSV Extractor：
    - streaming 7z
    - 只读 header 一次
    - 强制所有列 string
    """

    @staticmethod
    def _read_header(zfile: Path) -> list[str]:
        """
        只读取 CSV header（第一行）
        """
        proc = subprocess.Popen(
            ["7z", "x", "-so", str(zfile)],
            stdout=subprocess.PIPE,
        )

        # 只读第一行
        header = proc.stdout.readline()
        proc.kill()

        # Arrow CSV 默认用 ',' 分隔
        return header.decode("utf-8").strip().split(",")

    @staticmethod
    def open_reader(zfile: Path, streaming: bool = True):
        if not streaming:
            raise NotImplementedError("非 streaming 模式暂未实现")

        # ① 先读 header
        column_names = ExtractorEngine._read_header(zfile)

        # ② 再重新开启 streaming reader
        proc = subprocess.Popen(
            ["7z", "x", "-so", str(zfile)],
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
            column_names=column_names,
            block_size=1 << 25,   # 16MB
            use_threads=True,
        )

        return csv.open_csv(
            proc.stdout,          # binary stream
            read_options=read_opts,
            convert_options=convert_opts,
        )

    @staticmethod
    def cast_strings(batch: pa.RecordBatch) -> pa.RecordBatch:
        # 现在只是保险，不是救命
        cols = [col.cast(pa.string(), safe=False) for col in batch.columns]
        return pa.record_batch(cols, batch.schema.names)
