#!filepath: src/observability/progress.py
from src import logs


class ProgressReporter:
    """
    最轻量进度系统（不会影响 pytest、CI，不依赖 Rich/TQDM）
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled

    def start(self, task: str, total: int, unit: str = ""):
        if not self.enabled:
            return
        logs.info(f"[Progress] {task} started total={total} {unit}")

    def update(self, task: str, current: int, total: int, unit: str = ""):
        if not self.enabled:
            return
        logs.info(f"[Progress] {task}: {current}/{total} {unit}")

    def done(self, task: str):
        if not self.enabled:
            return
        logs.info(f"[Progress] {task} done")
