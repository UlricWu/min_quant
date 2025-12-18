#!filepath: src/l2/common/exchange_def.py

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass(frozen=True)
class ExchangeDefinition:
    """
    定义：如何从 SH/SZ 的 L2 字段 → InternalEvent 字段
    """

    time_field: str                   # TickTime 或 OrderTime
    event_field: str                  # TickType / OrderType / ExecType
    event_mapping: Dict[Any, str]     # 例如 {"A": "ADD", "D": "CANCEL"}
    price_field: str
    volume_field: str
    side_field: Optional[str]         # SH 有 Side，SZ 可能无
    side_mapping: Optional[Dict[Any, str]]
    id_field: str
    buy_no_field: Optional[str]
    sell_no_field: Optional[str]
