# src/pipeline/parallel/manifest.py
from __future__ import annotations


class ParallelManifest:
    """
    并行最小契约：
    - executor 只依赖这三个方法
    """

    def is_done(self, item: str) -> bool:
        raise NotImplementedError

    def mark_done(self, item: str) -> None:
        raise NotImplementedError

    def mark_failed(self, item: str, reason: str) -> None:
        raise NotImplementedError
