#!filepath: src/core/types.py
from __future__ import annotations
from dataclasses import dataclass
import pandas as pd


@dataclass
class TradeBatch:
    """
    统一的“逐笔成交批次”结构：
    - symbol: 六位代码字符串，如 '600000'
    - date: 'YYYY-MM-DD' 或 'YYYYMMDD'
    - df: 原始或已解析的逐笔 DataFrame
    """
    symbol: str
    date: str
    df: pd.DataFrame
