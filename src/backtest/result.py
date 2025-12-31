# src/backtest/result.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class BacktestResult:
    """
    BacktestResult (FINAL / FROZEN)

    不可变事实结果，用于：
      - 结果回放
      - 回归测试
      - Metrics 派生
    """

    # -----------------------
    # Experiment identity
    # -----------------------
    name: str
    level: str                # l1 / l2 / l3
    dates: List[str]
    symbols: List[str]
    replay: str               # single / multi
    strategy: Dict            # 原始 strategy config

    # -----------------------
    # Time / event stats
    # -----------------------
    start_ts: int
    end_ts: int
    n_events: int
    n_signals: int

    # -----------------------
    # Core trajectories
    # -----------------------
    equity_curve: List[float]          # 按 replay 顺序
    timestamps: List[int]              # 与 equity_curve 对齐

    # -----------------------
    # Trade facts (L2+)
    # -----------------------
    trades: Optional[List[Dict]] = None
    fills: Optional[List[Dict]] = None
