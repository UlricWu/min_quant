#!filepath: src/observability/instrumentation.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from contextlib import contextmanager
from collections import OrderedDict

from src.observability.progress import ProgressReporter
from src.observability.timer import Timer
from src.observability.metrics import MetricRecorder
from src.observability.context import InstrumentationContext
from src.observability.timeline_reporter import TimelineReporter
from src import logs


@dataclass
class Instrumentation:
    """
    Pipeline Observability 主总线：
    - 计时（Timer）
    - 进度（ProgressReporter）
    - 指标（MetricRecorder）
    - 上下文（InstrumentationContext）
    - Timeline（阶段耗时报告）
    """

    enabled: bool = True

    def __post_init__(self):
        self.progress = ProgressReporter(enabled=self.enabled)
        self.timeit = Timer(enabled=self.enabled)    # ← 避免与 timer() 方法冲突
        self.metrics = MetricRecorder(enabled=self.enabled)
        self.context = InstrumentationContext()

        # timeline: OrderedDict[step_name, seconds]
        self.timeline = OrderedDict()

    # ---------------------------------------------------------
    # Context Manager Timer (核心)
    # ---------------------------------------------------------
    def timer(self, name: str):
        inst = self

        @contextmanager
        def _ctx():
            inst.timeit.start(name)
            try:
                yield
            finally:
                elapsed = inst.timeit.end(name)
                inst.timeline[name] = elapsed
                logs.info(f"[Timer] {name} took {elapsed:.3f}s")

        return _ctx()

    # ---------------------------------------------------------
    # 输出 Pipeline Timeline（可打印 / 可保存）
    # ---------------------------------------------------------
    def generate_timeline_report(self, date: str):
        reporter = TimelineReporter(self.timeline, date)
        reporter.print()
