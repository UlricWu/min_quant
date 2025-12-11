#!filepath: src/dataloader/pipeline/context.py
from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict


@dataclass
class PipelineContext:
    """
    贯穿整个 pipeline 的上下文（仅保存状态，不做逻辑）
    """

    date: str

    raw_dir: Path
    parquet_dir: Path
    symbol_root: Path

    artifacts: Dict[str, Any] = field(default_factory=dict)
    meta: Dict[str, Any] = field(default_factory=dict)

    def set_artifact(self, key: str, value: Any) -> None:
        self.artifacts[key] = value

    def get_artifact(self, key: str, default=None) -> Any:
        return self.artifacts.get(key, default)
