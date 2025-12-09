#!filepath: src/observability/timer.py
import time
from typing import Dict


class Timer:
    """
    高精度计时器
    - start(name)
    - end(name) → 返回耗时秒数
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._start: Dict[str, float] = {}

    def start(self, name: str):
        if not self.enabled:
            return
        self._start[name] = time.perf_counter()

    def end(self, name: str) -> float:
        if not self.enabled:
            return 0.0
        if name not in self._start:
            return 0.0
        elapsed = time.perf_counter() - self._start.pop(name)
        return elapsed
