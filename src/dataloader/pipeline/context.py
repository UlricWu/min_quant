#!filepath: src/dataloader/pipeline/context.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipelineContext:
    date: str

    # Raw / vendor
    raw_dir: Path
    parquet_dir: Path

    # Canonical (after Normalize)
    normalize_dir: Path

    # Partitioned
    symbol_dir: Path
