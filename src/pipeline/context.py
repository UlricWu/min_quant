#!filepath: src/pipeline/context.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, Dict, Any
from src.engines.parser_engine import NormalizedEvent

import pyarrow as pa
from src.config.backtest_config import BacktestConfig

from src.config.backtest_config import BacktestConfig


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
    # 通用
    # offline
    key: str  # output_slot
    input_file: Path  # input
    output_file: Path  # output
    mode: str = "full"  # "full" | "order" | "trade"

    # mode: Literal["offline", "replay", "realtime"] = "offline"

    # # replay / realtime
    event: Optional[NormalizedEvent] = None

    # 控制
    emit_snapshot: bool = False
#
# @dataclass(slots=True)
# class BacktestContext:
#     """
#     BacktestContext（FINAL / FROZEN）
#
#     规则：
#       - 所有字段必须在这里显式声明
#       - Step 禁止注入新字段
#     """
#
#     # --------------------------------------------------
#     # Identity
#     # --------------------------------------------------
#     run_id: str
#     date: str
#
#     # --------------------------------------------------
#     # Directories
#     # --------------------------------------------------
#     backtest_dir: Path
#     meta_dir: Path
#     feature_dir: Path
#     label_dir: Path
#
#     # --------------------------------------------------
#     # Config
#     # --------------------------------------------------
#     cfg: BacktestConfig
#
#     # --------------------------------------------------
#     # Per-date data (由 LoadDataStep 填充)
#     # --------------------------------------------------
#     data_handler: Optional[FeatureDataHandler] = None
#
#     # --------------------------------------------------
#     # Cross-date accumulated state
#     # --------------------------------------------------
#     portfolio: Optional[Portfolio] = None
#     recorder: Optional[SimpleRecorder] = None
#
#     # --------------------------------------------------
#     # Run-final outputs
#     # --------------------------------------------------
#     result: Optional[BacktestResult] = None
#     metrics: Optional[Dict[str, Any]] = None  # ⭐ 新增（最后一个）
