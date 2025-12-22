# src/dataloader/pipeline/step.py
from __future__ import annotations

from pathlib import Path
from typing import Protocol

from src import logs
from src.pipeline.context import PipelineContext
from src.observability.instrumentation import Instrumentation, NoOpInstrumentation


class PipelineStep(Protocol):
    """
    所有 Pipeline Step 的统一接口。

    语义：
    - Step 是【时间语义边界】（scope），而不是 leaf 计时单元
    - Step 本身不进入 timeline，只用于定义 wall-time 区间

    输入：PipelineContext
    输出：PipelineContext（可以原样返回或写入附加信息）
    """

    def run(self, ctx: PipelineContext) -> PipelineContext:
        ...


class BasePipelineStep:
    """
    Pipeline Step 基类（推荐使用）。

    规范约束：
    1. Step 必须使用父级 timer（record=False）
    2. Step 不允许直接进入 timeline
    3. Leaf 计时只能发生在 Step 内部
    4. Step 支持“存在即跳过”的缓存语义
    """

    def __init__(self, inst: Instrumentation | None = None):
        self.inst: Instrumentation | NoOpInstrumentation = (
            inst if inst is not None else NoOpInstrumentation()
        )

    @property
    def step_name(self) -> str:
        """默认使用类名作为 Step 名称。"""
        return self.__class__.__name__

    # -----------------------------------------------------
    # Step-level timer（父级 scope，不进 timeline）
    # -----------------------------------------------------
    def timed(self):
        """
        Step 级计时器（父级 scope）。

        语义说明：
        - 仅用于定义 Step 的 wall-time 区间
        - 不记录到 timeline（record=False）
        - 防止 Step 与其内部 leaf 计时重复统计
        """
        if self.inst is None:
            return _DummyTimer()
        return self.inst.timer(self.step_name, record=False)

    # -----------------------------------------------------
    # ⭐ Step 主输出（用于 skip 判断）
    # -----------------------------------------------------
    def output_path(self, ctx: PipelineContext) -> Path | None:
        """
        返回 Step 的“主输出文件路径”。

        - None 表示该 Step 不支持 skip
        - 子类应当 override
        """
        return None
# -----------------------------------------------------
    # ⭐ 是否跳过本 Step
    # -----------------------------------------------------
    def should_skip(self, ctx: PipelineContext) -> bool:
        """
        Step 级 skip 规则：

        - 仅当 output_path 存在时跳过
        - 不进入 Step timer
        """
        out = self.output_path(ctx)
        if out is None:
            return False

        if out.exists():
            logs.info(f"[SKIP] {self.step_name} output exists: {out}")
            return True

        return False

class _DummyTimer:
    """
    Instrumentation 为 None 时的 no-op timer。

    目的：
    - 保证调用方代码结构一致
    - 不引入任何运行期开销
    """

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc, tb):
        pass


# =========================================================
# 使用示例（规范示意）
# =========================================================
"""
class CsvConvertStep(BasePipelineStep):
    def run(self, ctx: PipelineContext) -> PipelineContext:
        with self.timed():  # Step scope（不进 timeline）
            with self.inst.timer("SZ_ORDER"):
                run_sz_order(ctx)

            with self.inst.timer("SH_MIXED"):
                run_sh_mixed(ctx)

            with self.inst.timer("SZ_TRADE"):
                run_sz_trade(ctx)
        return ctx
"""
