# src/engines/context.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal, Optional

from src.l2.common.normalized_event import NormalizedEvent


@dataclass(slots=True)
class EngineContext:
    """
    Engine 的唯一输入载体

    核心原则：
    - offline / replay / realtime 只是 mode 差异
    - execute(ctx) 永远只看 ctx
    """

    mode: Literal["offline", "replay", "realtime"]

    # 通用
    symbol: str
    date: Optional[str] = ''

    # offline
    input_path: Optional[Path] = ''
    output_path: Optional[Path] = ''

    # replay / realtime
    event: Optional[NormalizedEvent] = None

    # 控制
    emit_snapshot: bool = False
