import pyarrow as pa
from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine, US_PER_MINUTE

import pytest


def test_minute_trade_agg_rejects_unsorted_ts():
    """
    冻结契约：

    MinuteTradeAggEngine 假设输入 trade 已按 ts 升序排序。
    若输入乱序，则 open / close 的语义不再成立，
    调用方必须保证排序。

    本测试用于显式暴露该前提，防止误用。
    """
    engine = MinuteTradeAggEngine()
    base = 5 * US_PER_MINUTE

    table = pa.table(
        {
            "ts": [base + 20, base + 10],
            "price": [10.2, 10.0],
            "volume": [10, 10],
            "notional": [102.0, 100.0],
            "symbol": ["600000", "600000"],
        }
    )

    with pytest.raises(ValueError, match="requires input sorted by ts"):
        engine.execute(table)
