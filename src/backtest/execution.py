# backtest/execution.py
from __future__ import annotations
from src.backtest.events import OrderEvent, MarketEvent, FillEvent


class ExecutionEngine:
    """
    Level 1 Execution：
    - bar 驱动
    - 无状态
    """

    def __init__(self, commission_per_trade: float = 0.0):
        self.commission = commission_per_trade

    def execute(self, order: OrderEvent, market: MarketEvent) -> FillEvent:
        price = market.close  # 冻结：用 close 成交
        qty = order.quantity * order.direction

        return FillEvent(
            ts=market.ts,
            symbol=order.symbol,
            quantity=qty,
            price=price,
            commission=self.commission,
        )
