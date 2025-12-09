#!filepath: src/observability/context.py
from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass
class InstrumentationContext:
    """
    存储 pipeline 运行过程中的上下文信息：
    - 当前 step
    - 当前 symbol
    - 当前文件
    """

    state: Dict[str, Any] = field(default_factory=dict)

    def set(self, key: str, value: Any):
        self.state[key] = value

    def get(self, key: str, default=None):
        return self.state.get(key, default)
