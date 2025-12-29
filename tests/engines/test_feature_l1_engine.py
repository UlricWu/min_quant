from __future__ import annotations

import pyarrow as pa
import pandas as pd
import pytest

from src.engines.feature_l1_norm_engine import FeatureL1NormEngine


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

    engine = FeatureL1NormEngine(window=2)
    out = engine.execute(table)

    assert out.num_rows == table.num_rows
    assert "l1_z_w2_l0_amount" in out.column_names
    assert "l0_amount" in out.column_names


# -----------------------------------------------------------------------------
# 2. 防泄露测试：当前值不参与 rolling 统计
# -----------------------------------------------------------------------------
def test_l1_no_future_leakage():
    """
    极端可检测序列：
        l0_amount = [0, 0, 0, 100]
    正确实现下，最后一行 z-score 必须为 0
    """
    table = table_from_l0([0, 0, 0, 100])

    engine = FeatureL1NormEngine(window=3)
    out = engine.execute(table)

    df = out.to_pandas()

    assert df["l1_z_w3_l0_amount"].iloc[-1] == 0.0


# -----------------------------------------------------------------------------
# 3. window 行数语义测试
# -----------------------------------------------------------------------------
def test_l1_window_min_periods_behavior():
    """
    window = 3
    前 window 行（历史不足）全部为 0
    """
    table = table_from_l0([1, 2, 3, 4])

    engine = FeatureL1NormEngine(window=3)
    out = engine.execute(table)

    z = out.to_pandas()["l1_z_w3_l0_amount"]

    assert z.iloc[0] == 0.0
    assert z.iloc[1] == 0.0
    assert z.iloc[2] == 0.0
    assert isinstance(z.iloc[3], float)


# -----------------------------------------------------------------------------
# 4. std=0 数值稳定性
# -----------------------------------------------------------------------------
def test_l1_std_zero_handling():
    """
    历史值恒定 -> std = 0
    z-score 必须稳定为 0
    """
    table = table_from_l0([5, 5, 5, 5, 5])

    engine = FeatureL1NormEngine(window=2)
    out = engine.execute(table)

    z = out.to_pandas()["l1_z_w2_l0_amount"]

    assert (z == 0.0).all()


# -----------------------------------------------------------------------------
# 5. append 模式安全性（多 window 并存）
# -----------------------------------------------------------------------------
def test_l1_append_does_not_override_existing_columns():
    """
    多个 FeatureL1NormEngine(window=*) 叠加：
      - 不覆盖已有 L1 列
      - window 是 schema 的一部分
    """
    table = table_from_l0([1, 2, 3, 4])

    engine_w2 = FeatureL1NormEngine(window=2)
    out1 = engine_w2.execute(table)

    engine_w3 = FeatureL1NormEngine(window=3)
    out2 = engine_w3.execute(out1)

    cols = out2.column_names

    assert "l0_amount" in cols
    assert "l1_z_w2_l0_amount" in cols
    assert "l1_z_w3_l0_amount" in cols


# -----------------------------------------------------------------------------
# 6. 空表安全性
# -----------------------------------------------------------------------------
def test_l1_empty_table_is_noop():
    """
    空表必须：
      - 不报错
      - 不新增列
    """
    empty = table_from_l0([])

    engine = FeatureL1NormEngine(window=5)
    out = engine.execute(empty)

    assert out.num_rows == 0
    assert out.column_names == ["l0_amount"]
