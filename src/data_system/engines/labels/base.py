#!filepath: src/engines/labels/base.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Sequence

import pyarrow as pa


class BaseLabelEngine(ABC):
    """
    BaseLabelEngine（FINAL / FROZEN）

    定位：
      - 纯 row-based 计算
      - 不关心时间语义
      - 不关心 bar / tick / trade
      - 不关心配置来源
    """

    @abstractmethod
    def label_columns(self) -> Sequence[str]:
        """Engine 产生的 label 列名"""
        raise NotImplementedError

    @abstractmethod
    def execute(self, table: pa.Table) -> pa.Table:
        """
        输入：
          - 单 symbol
          - 行顺序已保证

        输出：
          - 行数不变
          - append label 列
        """
        raise NotImplementedError


def require_columns(table: pa.Table, cols: Sequence[str], *, who: str) -> None:
    missing = [c for c in cols if c not in table.column_names]
    if missing:
        raise ValueError(f"[{who}] missing required columns: {missing}")
