#!filepath: src/pipeline/context.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional
from src.engines.parser_engine import NormalizedEvent


@dataclass
class PipelineContext:
    """
    PipelineContext = Pipeline 运行期唯一上下文（只读语义）

    设计原则：
    - Pipeline 负责构造
    - Step 只读 / 传递
    - 不放业务逻辑
    """

    # -------------------------
    # time
    # -------------------------
    date: str

    # -------------------------
    # resolved dirs（方便使用）
    # -------------------------
    raw_dir: Path
    parquet_dir: Path
    fact_dir: Path
    meta_dir: Path

    feature_dir: Path
    label_dir: Path
    # -------- PipelineRuntime flags --------
    abort_pipeline: bool = False
    abort_reason: Optional[str] = None


@dataclass(slots=True)
class EngineContext:
    """
    Engine 的唯一输入载体

    核心原则：
    - offline / replay / realtime 只是 mode 差异
    - execute(ctx) 永远只看 ctx
    """

    mode: Literal["offline", "replay", "realtime"] = "offline"

    # 通用
    # offline
    input_path: Optional[Path] = Path
    output_path: Optional[Path] = Path

    # # replay / realtime
    event: Optional[NormalizedEvent] = None

    # 控制
    emit_snapshot: bool = False
