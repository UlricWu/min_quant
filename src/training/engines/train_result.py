from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class TrainResult:
    """
    TrainResult（FINAL / FROZEN）

    语义：
    - 一次完整训练的纯内存态结果
    - 不包含任何 I/O 语义
    """
    model: Any
    metrics: Dict[str, Any]