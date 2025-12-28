from __future__ import annotations

import pyarrow as pa
import pandas as pd
import pytest

from src.engines.feature_l1_engine import FeatureL1Engine


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def table_from_l0(values: list[float]) -> pa.Table:
    """
    构造一个最小 FeatureL0 Arrow Table
    - 单列 l0_amount
    - 行顺序即时间顺序
    """
    df = pd.DataFrame(
        {
            "l0_amount": values,
        }
    )
    return pa.Table.from_pandas(df, preserve_index=False)


# -----------------------------------------------------------------------------
# 1. 基本契约：行数不变 + 列追加
# -----------------------------------------------------------------------------
def test_l1_preserves_row_count_and_appends_column():
    table = table_from_l0([1, 2, 3, 4, 5])

    engine = FeatureL1Engine(window=2)
    out = engine.execute(table)

    assert out.num_rows == table.num_rows
    assert "l1_z_w2_amount" in out.column_names
    assert "l0_amount" in out.column_names


# -----------------------------------------------------------------------------
# 2. 防泄露测试：当前值不参与 rolling 统计
# -----------------------------------------------------------------------------
def test_l1_no_future_leakage():
    """
    构造一个“极端可检测”的序列：

        l0_amount = [0, 0, 0, 100]

    window = 3

    若错误地使用当前值参与 rolling：
        最后一行 mean > 0, z != 0

    正确逻辑（只用历史）：
        历史全是 0 -> std = 0 -> z = 0
    """
    table = table_from_l0([0, 0, 0, 100])

    engine = FeatureL1Engine(window=3)
    out = engine.execute(table)

    df = out.to_pandas()

    assert df["l1_z_w3_amount"].iloc[-1] == 0.0


# -----------------------------------------------------------------------------
# 3. window 行数语义测试
# -----------------------------------------------------------------------------
def test_l1_window_min_periods_behavior():
    """
    window = 3
    前 window 行（不足历史）应全部为 0
    """
    table = table_from_l0([1, 2, 3, 4])

    engine = FeatureL1Engine(window=3)
    out = engine.execute(table)

    z = out.to_pandas()["l1_z_w3_amount"]

    assert z.iloc[0] == 0.0
    assert z.iloc[1] == 0.0
    assert z.iloc[2] == 0.0
    # 第 4 行开始才可能非 0
    assert isinstance(z.iloc[3], float)


# -----------------------------------------------------------------------------
# 4. std=0 数值稳定性
# -----------------------------------------------------------------------------
def test_l1_std_zero_handling():
    """
    当历史值恒定时，std = 0
    结果应稳定为 0，而不是 inf / nan
    """
    table = table_from_l0([5, 5, 5, 5, 5])

    engine = FeatureL1Engine(window=2)
    out = engine.execute(table)

    z = out.to_pandas()["l1_z_w2_amount"]

    assert (z == 0.0).all()


# -----------------------------------------------------------------------------
# 5. append 模式安全性（已有列不被破坏）
# -----------------------------------------------------------------------------
def test_l1_append_does_not_override_existing_columns():
    table = table_from_l0([1, 2, 3, 4])

    engine_w2 = FeatureL1Engine(window=2)
    out1 = engine_w2.execute(table)

    engine_w3 = FeatureL1Engine(window=3)
    out2 = engine_w3.execute(out1)

    cols = out2.column_names
