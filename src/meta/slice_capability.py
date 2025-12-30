# src/meta/slice_capability.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple, Literal

SliceType = Literal["symbol"]  # 预留 future: time / bucket / etc.


@dataclass(frozen=True)
class SliceCapability:
    """
    SliceCapability（FINAL / FROZEN）

    角色：
      - 表达“我能按什么维度 slice”
      - 封装 index + canonical table
      - 提供统一的访问入口

    示例：
      - SymbolSliceCapability
      - DateSliceCapability
      - SymbolMinuteSliceCapability（未来）
    """

    # ----------------------------------
    type: SliceType
    index: Dict[str, Tuple[int, int]]  # key -> (start, length)

    # --------------------------------------------------
    def keys(self) -> list[str]:
        return list(self.index.keys())

    def bounds(self, key: str) -> Tuple[int, int]:
        return self.index[key]
