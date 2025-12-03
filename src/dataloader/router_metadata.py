#!filepath: src/dataloader/router_metadata.py
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import json
import pyarrow as pa
import pyarrow.parquet as pq

from src.utils.filesystem import FileSystem
from src.utils.path import PathManager
from src import logs


class RouterMetadata:
    """
    记录 SymbolRouter 的处理元信息
    输出两个文件：
        dataloader/metadata/<date>.json
        dataloader/metadata/<date>.parquet
    """

    def __init__(self):
        self.reset()

    def reset(self):
        self.input_parquets: List[Dict] = []
        self.symbol_output: List[Dict] = []
        self.filtered_symbols: List[str] = []

    # -----------------------------------------------------
    def add_input_file(self, name: str, rows: int):
        self.input_parquets.append({
            "file": name,
            "rows": rows
        })

    def add_symbol_output(self, symbol: str, rows: int):
        self.symbol_output.append({
            "symbol": symbol,
            "rows": rows
        })

    def add_filtered(self, sid: str):
        self.filtered_symbols.append(sid)

    # -----------------------------------------------------
    def save(self, date: str):
        out_dir = PathManager.data_dir() / "metadata"
        FileSystem.ensure_dir(out_dir)

        json_path = out_dir / f"{date}.json"

        rows = sum(x["rows"] for x in self.symbol_output)

        if rows == 0:
            return
        summary = {
            "date": date,
            "time": datetime.now().isoformat(),
            "input_files": self.input_parquets,
            "symbols": self.symbol_output,
            "filtered_symbols": sorted(set(self.filtered_symbols)),
            "symbols_count": len(self.symbol_output),
            "rows_total": rows,
        }

        # ----- 写 JSON -----
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        logs.info(f"[RouterMetadata] 写入 JSON: {json_path}")
