#!filepath: src/dataloader/streaming_csv_split_writer/extractor.py

from __future__ import annotations
import subprocess
from pathlib import Path

from src import logs


class CsvExtractor:
    """
    只负责将 .7z streaming 解压为 CSV 字节流
    """

    @logs.catch()
    def extract(self, zfile: Path):
        proc = subprocess.Popen(
            ["7z", "x", "-so", "-mmt=on", str(zfile)],
            stdout=subprocess.PIPE
        )
        return proc.stdout
