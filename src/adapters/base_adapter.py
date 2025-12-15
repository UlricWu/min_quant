from __future__ import annotations
from src.observability.instrumentation import Instrumentation


class BaseAdapter:
    """
    Adapter 的通用接口。

    - 持有 Instrumentation（可选）
    - 提供 timed() 方便在内部对关键区域计时
    """

    def __init__(self, inst: Instrumentation | None = None):
        self.inst = inst

    def timer(self, name: str=''):
        """
        Adapter 内部计时：
            with adapter.timer("write_parquet"):
                writer.write_batch(...)

        Instrumentation 为 None 时自动禁用计时。
        """
        if not name:
            name = self.__class__.__name__
        if self.inst is None:
            return _NoOpTimer()
        return self.inst.timer(name)


class _NoOpTimer:
    """inst 为 None，则计时器为 no-op。"""
    def __enter__(self): pass
    def __exit__(self, exc_type, exc, tb): pass
