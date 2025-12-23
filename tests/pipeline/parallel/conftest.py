# tests/pipeline/parallel/conftest.py
from __future__ import annotations
from typing import Dict
import threading
import pytest

class FakeManifest:
    """
    线程 / 进程安全的 in-memory manifest
    """
    def __init__(self):
        self._done: Dict[str, bool] = {}
        self._failed: Dict[str, str] = {}
        self._lock = threading.Lock()

    def is_done(self, item: str) -> bool:
        with self._lock:
            return self._done.get(item, False)

    def mark_done(self, item: str) -> None:
        with self._lock:
            self._done[item] = True

    def mark_failed(self, item: str, reason: str) -> None:
        with self._lock:
            self._failed[item] = reason

    # ---------- 断言辅助 ----------
    def done_items(self) -> set[str]:
        return {k for k, v in self._done.items() if v}

    def failed_items(self) -> set[str]:
        return set(self._failed.keys())
# ✅ 关键：fixture 定义
@pytest.fixture
def fake_manifest() -> FakeManifest:
    return FakeManifest()


