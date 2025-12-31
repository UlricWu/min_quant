from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Literal, List, Dict


class BacktestConfig(BaseModel):
    """
    BacktestConfig（FINAL / FROZEN）

    语义：
      - 回测“实验定义”
      - symbols 由 backtest 决定（全量数据上做子集选择）
      - 不定义路径
    """

    # 实验名
    name: str = "default"

    # 使用哪些数据版本
    dates: List[str]

    # 使用哪些 symbols（本次实验）
    symbols: List[str] = Field(..., min_length=1)
    # 本次 backtest 要“在哪些 symbol 上运行”


    
    # 回测层级
    level: Literal["l1", "l2", "l3"] = "l1"

    # replay policy
    replay: Literal["single", "multi"] = "single"

    # strategy 参数（opaque）
    strategy: Dict
