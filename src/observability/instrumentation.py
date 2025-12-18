#!filepath: src/observability/instrumentation.py
from __future__ import annotations

from dataclasses import dataclass
from contextlib import contextmanager
from collections import OrderedDict
from typing import Dict

from src.observability.progress import ProgressReporter
from src.observability.timer import Timer
from src.observability.metrics import MetricRecorder
from src.observability.context import InstrumentationContext
from src.observability.timeline_reporter import TimelineReporter
from src import logs


@dataclass
class Instrumentation:
    """
    最终版 Instrumentation（Leaf-only accounting + Parent scope）。

    设计铁律：
    1. Timeline 只记录【叶子节点】（record=True）
    2. Step / 父级 timer 仅作为时间语义边界（record=False）
    3. record=False 的 timer 不产生任何副作用
    4. Instrumentation 本身不在热路径打日志
    """

    enabled: bool = True

    def __post_init__(self):
        self.progress = ProgressReporter(enabled=self.enabled)
        self._timer = Timer(enabled=self.enabled)
        self.metrics = MetricRecorder(enabled=self.enabled)
        self.context = InstrumentationContext()

        # timeline: OrderedDict[leaf_name, elapsed_seconds]
        self.timeline: Dict[str, float] = OrderedDict()

    # ---------------------------------------------------------
    # Context Manager Timer（唯一入口）
    # ---------------------------------------------------------
    def timer(self, name: str, *, record: bool = True):
        """
        Context-manager timer.

        Parameters
        ----------
        name : str
            计时名称
        record : bool
            - True  : 叶子节点，记录到 timeline
            - False : 父级 scope，仅定义 wall-time（不产生副作用）
        """
        inst = self

        @contextmanager
        def _ctx():
            if not inst.enabled:
                yield
                return

            inst._timer.start(name)
            try:
                yield
            finally:
                elapsed = inst._timer.end(name)

                # 只允许 record=True 的叶子节点写入 timeline
                if record:
                    inst.timeline[name] = elapsed

        return _ctx()

    # ---------------------------------------------------------
    # Timeline 输出（冷路径）
    # ---------------------------------------------------------
    def generate_timeline_report(self, date: str):
        reporter = TimelineReporter(self.timeline, date)
        reporter.print()


# -------------------------------------------------------------
# No-op Instrumentation（禁用 observability）
# -------------------------------------------------------------
class NoOpInstrumentation:
    """Instrumentation disabled 时使用。"""

    def timer(self, name: str, *, record: bool = True):
        return _NoOpTimer()


class _NoOpTimer:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc, tb):
        pass
