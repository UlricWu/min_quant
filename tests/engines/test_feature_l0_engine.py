from __future__ import annotations

import pyarrow as pa
import pandas as pd
import pytest

from src.engines.feature_l0_engine import FeatureL0Engine
import numpy as np


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def table_from_rows(rows: list[dict]) -> pa.Table:
    """
    用 dict rows 构造 Arrow Table，避免 pandas 隐式行为
    """
    return pa.Table.from_pydict(
        {k: [r[k] for r in rows] for k in rows[0]}
    )


# -----------------------------------------------------------------------------
# 1. 输入契约校验
# -----------------------------------------------------------------------------
def test_l0_missing_required_columns_raises():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 2.0,
                "low": 1.0,
                "close": 1.5,
                # volume / trade_count 缺失
            }
        ]
    )

    with pytest.raises(ValueError):
        engine.execute(table)


# -----------------------------------------------------------------------------
# 2. 行数保持不变
# -----------------------------------------------------------------------------
def test_l0_preserves_row_count():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 2.0,
                "low": 1.0,
                "close": 1.5,
                "volume": 10,
                "trade_count": 5,
            },
            {
                "open": 2.0,
                "high": 3.0,
                "low": 2.0,
                "close": 2.5,
                "volume": 20,
                "trade_count": 10,
            },
        ]
    )

    out = engine.execute(table)

    assert out.num_rows == table.num_rows


# -----------------------------------------------------------------------------
# 3. l0_amount 计算正确
# -----------------------------------------------------------------------------
def test_l0_amount_computation():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 1.5,
                "low": 1.0,
                "close": 1.2,
                "volume": 10,
                "trade_count": 2,
            }
        ]
    )

    out = engine.execute(table)
    df = out.to_pandas()

    assert df["l0_amount"].iloc[0] == pytest.approx(1.2 * 10)


# -----------------------------------------------------------------------------
# 4. l0_avg_trade_size 数值稳定性
# -----------------------------------------------------------------------------
def test_l0_avg_trade_size_zero_trade_count():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 1.2,
                "low": 0.9,
                "close": 1.0,
                "volume": 0,
                "trade_count": 0,
            }
        ]
    )

    out = engine.execute(table)
    df = out.to_pandas()

    assert df["l0_avg_trade_size"].iloc[0] == 0.0


# -----------------------------------------------------------------------------
# 5. buy/sell 特征存在时才生成
# -----------------------------------------------------------------------------
def test_l0_buy_sell_features_present_only_if_columns_exist():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 1.2,
                "low": 1.0,
                "close": 1.1,
                "volume": 10,
                "trade_count": 5,
                "buy_volume": 6,
                "sell_volume": 4,
            }
        ]
    )

    out = engine.execute(table)
    cols = out.column_names

    assert "l0_buy_ratio" in cols
    assert "l0_imbalance" in cols


def test_l0_buy_sell_features_absent_if_missing_columns():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 1.2,
                "low": 1.0,
                "close": 1.1,
                "volume": 10,
                "trade_count": 5,
            }
        ]
    )

    out = engine.execute(table)
    cols = out.column_names

    assert "l0_buy_ratio" not in cols
    assert "l0_imbalance" not in cols


# -----------------------------------------------------------------------------
# 6. l0_range / l0_abs_move 计算正确
# -----------------------------------------------------------------------------
def test_l0_price_range_and_abs_move():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 2.0,
                "low": 0.5,
                "close": 1.5,
                "volume": 10,
                "trade_count": 5,
            }
        ]
    )

    out = engine.execute(table)
    df = out.to_pandas()

    assert df["l0_range"].iloc[0] == pytest.approx(2.0 - 0.5)
    assert df["l0_abs_move"].iloc[0] == pytest.approx(abs(1.5 - 1.0))


# -----------------------------------------------------------------------------
# 7. log 特征数值安全
# -----------------------------------------------------------------------------
def test_l0_log_features_non_negative_and_defined():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 0,
                "trade_count": 0,
            }
        ]
    )

    out = engine.execute(table)
    df = out.to_pandas()

    assert df["l0_log_volume"].iloc[0] == 0.0
    assert df["l0_log_trade_count"].iloc[0] == 0.0


# -----------------------------------------------------------------------------
# 8. append / replace 行为正确
# -----------------------------------------------------------------------------
def test_l0_append_or_replace_behavior():
    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 1.2,
                "low": 0.9,
                "close": 1.1,
                "volume": 10,
                "trade_count": 5,
            }
        ]
    )

    out1 = engine.execute(table)
    out2 = engine.execute(out1)

    # 再跑一次，不应产生重复列
    assert out1.column_names == out2.column_names


# -----------------------------------------------------------------------------
# 9. L0 数值健康性：禁止 NaN / Inf
# -----------------------------------------------------------------------------
def test_l0_numeric_columns_have_no_nan_or_inf():
    """
    FeatureL0 数值健康性测试（冻结）：

    - 所有 l0_* 列
    - 不允许 NaN
    - 不允许 +Inf / -Inf

    该测试确保：
    - L0 层输出的物理特征是“数值干净”的
    - 不将数值异常泄漏到 L1/L2
    """

    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 2.0,
                "low": 1.0,
                "close": 1.5,
                "volume": 0,
                "trade_count": 0,
                "buy_volume": 0,
                "sell_volume": 0,
            },
            {
                "open": 2.0,
                "high": 3.0,
                "low": 2.0,
                "close": 2.5,
                "volume": 10,
                "trade_count": 5,
                "buy_volume": 6,
                "sell_volume": 4,
            },
        ]
    )

    out = engine.execute(table)
    df = out.to_pandas()

    # 只检查 l0_* 数值列
    l0_cols = [c for c in df.columns if c.startswith("l0_")]

    assert l0_cols, "No l0_* columns found"

    for col in l0_cols:
        series = df[col]

        # NaN 检查
        assert not series.isna().any(), f"{col} contains NaN"

        # Inf 检查
        assert not (series == float("inf")).any(), f"{col} contains +Inf"
        assert not (series == float("-inf")).any(), f"{col} contains -Inf"


# -----------------------------------------------------------------------------
# 10. L0 数值健康性（最强约束）：所有值必须 finite
# -----------------------------------------------------------------------------
def test_l0_numeric_columns_are_all_finite():
    """
    FeatureL0 数值健康性（冻结）：

    - 所有 l0_* 列
    - 必须满足 np.isfinite == True
    - 覆盖 NaN / +Inf / -Inf 的统一约束

    这是 L0 层的最终数值封闭性保证。
    """

    engine = FeatureL0Engine()

    table = table_from_rows(
        [
            {
                "open": 1.0,
                "high": 2.0,
                "low": 1.0,
                "close": 1.5,
                "volume": 0,
                "trade_count": 0,
                "buy_volume": 0,
                "sell_volume": 0,
            },
            {
                "open": 2.0,
                "high": 3.0,
                "low": 2.0,
                "close": 2.5,
                "volume": 10,
                "trade_count": 5,
                "buy_volume": 6,
                "sell_volume": 4,
            },
        ]
    )

    out = engine.execute(table)
    df = out.to_pandas()

    l0_cols = [c for c in df.columns if c.startswith("l0_")]
    assert l0_cols, "No l0_* columns found"

    for col in l0_cols:
        values = df[col].to_numpy()

        assert values.dtype.kind in {"f", "i"}, f"{col} is not numeric"

        assert (
            np.isfinite(values).all()
        ), f"{col} contains non-finite values"
