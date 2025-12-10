#!filepath: src/observability/timeline_reporter.py
from typing import Dict
from src import logs


class TimelineReporter:
    """
    Pipeline Timeline 报告：
    - step → 耗时秒数
    """

    def __init__(self, timeline: Dict[str, float], date: str):
        self.timeline = timeline
        self.date = date

    def print(self):
        logs.info(f"[Timeline] ===== Pipeline timeline for {self.date} =====")

        total = 0.0
        for name, sec in self.timeline.items():
            logs.info(f"[Timeline] {name:<30} {sec:>8.3f}s")
            total += sec

        logs.info(f"[Timeline] Total{'':<27} {total:>8.3f}s")
        logs.info("[Timeline] ===========================================")
