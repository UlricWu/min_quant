#!filepath: src/engines/base_adapter.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Iterable, Any, Dict


InEvent = TypeVar("InEvent")
OutEvent = TypeVar("OutEvent")


class BaseEngine(ABC, Generic[InEvent, OutEvent]):
    """
    Engine 抽象基类（Atomic Engine Layer）：

    - 不做任何 I/O（不读写 parquet / 文件 / socket）
    - 专注“输入事件 → 输出事件”的纯逻辑
    - 可被 Offline / Realtime / Backtest 复用
    """

    @abstractmethod
    def process(self, event: InEvent) -> OutEvent:
        """
        处理单个事件（最小粒度单位）。
        """
        raise NotImplementedError

    def process_stream(self, events: Iterable[InEvent]) -> Iterable[OutEvent]:
        """
        流式处理一批事件，默认逐个调用 process。
        如需状态机可在子类中覆写。
        """
        for ev in events:
            yield self.process(ev)
