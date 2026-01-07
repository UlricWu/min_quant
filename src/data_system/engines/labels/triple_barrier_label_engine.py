#!filepath: src/engines/labels/triple_barrier_label_engine.py
from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from src.data_system.engines.labels.base import BaseLabelEngine, LabelSpec, require_columns


@dataclass(frozen=True)
class TripleBarrierLabelConfig:
    """
    Triple-Barrier Label（冻结配置）

    语义：在未来 max_holding 步内，价格路径是否先触及
      - +pt（止盈 barrier）
      - -sl（止损 barrier）
      - 或超时（0）

    说明：
      - 这是“交易结果”风格 label，强依赖执行假设
      - 你现在阶段建议只冻结接口，不启用
    """
    max_holding: int = 20
    pt: float = 0.01
    sl: float = 0.005
    price_col: str = "close"
    col_name: str | None = None


class TripleBarrierLabelEngine(BaseLabelEngine):
    """
    TripleBarrierLabelEngine（接口冻结 / 实现占位）

    输入契约：
      - 单日、单 symbol、有序
      - 至少包含 ts, symbol, price_col

    输出：
      - append 一列 int8 label：+1 / 0 / -1
      - 行数不变，尾部 max_holding 行可能为 null/0（实现可选）
    """

    def __init__(self, cfg: TripleBarrierLabelConfig):
        if cfg.max_holding <= 0:
            raise ValueError("max_holding must be positive")
        if cfg.pt <= 0 or cfg.sl <= 0:
            raise ValueError("pt/sl must be positive")
        self._cfg = cfg
        col = cfg.col_name or f"label_tb_m{cfg.max_holding}_pt{cfg.pt}_sl{cfg.sl}"
        self._spec = LabelSpec(name="triple_barrier_v1", columns=(col,))

    @property
    def spec(self) -> LabelSpec:
        return self._spec

    def execute(self, table: pa.Table) -> pa.Table:
        require_columns(table, ["ts", "symbol", self._cfg.price_col], who=self.__class__.__name__)

        # 这里先冻结接口，不在当前阶段实现复杂逻辑，避免引入执行假设与研究噪声。
        # 未来：你可以在此用 Arrow / numpy 实现路径扫描（单日内），并输出 +1/0/-1。
        raise NotImplementedError(
            "TripleBarrierLabelEngine is intentionally not implemented at current stage. "
            "Interface is frozen; implement when Level-3 execution semantics are ready."
        )
