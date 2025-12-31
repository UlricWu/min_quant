#!filepath: src/pipeline/context.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional, Dict
from src.engines.parser_engine import NormalizedEvent

import pyarrow as pa
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
    # 通用
    # offline
    key: str  # output_slot
    input_file: Path  # input
    output_file: Path  # output
    mode: str ="full" # "full" | "order" | "trade"

    # mode: Literal["offline", "replay", "realtime"] = "offline"

    # # replay / realtime
    event: Optional[NormalizedEvent] = None

    # 控制
    emit_snapshot: bool = False

from src.backtest.result import BacktestResult


@dataclass
class BacktestContext:
    """
    BacktestContext（FINAL / FROZEN）

    设计原则：
      - Step 之间唯一通信载体
      - 只存“事实 / 中间态”，不存业务逻辑
      - offline / l1 / l2 / l3 通用
    """

    # -------------------------
    # identity
    # -------------------------
    date: str
    backtest_dir: Path
    meta_dir: Path

    # -------------------------
    # data layer
    # -------------------------
    tables: Optional[Dict[str, pa.Table]] = None
    data_handler: Optional[object] = None

    # -------------------------
    # execution layer
    # -------------------------
    portfolio: Optional[object] = None

    # -------------------------
    # result layer
    # -------------------------
    result: Optional[BacktestResult] = None
    metrics: Optional[dict] = None