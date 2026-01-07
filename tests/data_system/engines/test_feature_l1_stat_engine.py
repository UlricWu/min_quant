from __future__ import annotations

import math

import pandas as pd
import pyarrow as pa
import pytest

from src.data_system.engines.feature_l1_stat_engine import FeatureL1StatEngine


# -----------------------------------------------------------------------------
# helpers
# -----------------------------------------------------------------------------
def table_from_l0(
    *,
    volume: list[float] | None = None,
    trade_count: list[int] | None = None,
    abs_move: list[float] | None = None,
    close: list[float] | None = None,
    range_: list[float] | None = None,
) -> pa.Table:
    """
    构造最小 FeatureL0 Arrow Table（单 symbol，时间已排序）
    只放 engine 可能用到的列
    """
    data: dict[str, list] = {}

    if volume is not None:
        data["l0_volume"] = volume
    if trade_count is not None:
        data["l0_trade_count"] = trade_count
    if abs_move is not None:
        data["l0_abs_move"] = abs_move
    if range_ is not None:
        data["l0_range"] = range_
    if close is not None:
        data["close"] = close

    df = pd.DataFrame(data)
    return pa.Table.from_pandas(df, preserve_index=False)


# -----------------------------------------------------------------------------
# 1. 基本契约：行数不变 + 列追加
# -----------------------------------------------------------------------------
def test_l1_stat_preserves_row_count_and_appends_columns():
    table = table_from_l0(
        volume=[10, 20, 30, 40],
        trade_count=[1, 2, 3, 4],
        abs_move=[1.0, 2.0, 3.0, 4.0],
    )

    engine = FeatureL1StatEngine(window=2)
    out = engine.execute(table)

    assert out.num_rows == table.num_rows

    cols = out.column_names
    assert "l1_mean_w2_volume" in cols
    assert "l1_sum_w2_trade_count" in cols
    assert "l1_std_w2_abs_move" in cols


# -----------------------------------------------------------------------------
# 2. 防未来泄露：rolling 只用历史
# -----------------------------------------------------------------------------
def test_l1_stat_no_future_leakage():
    """
    volume = [0, 0, 0, 100]
    window = 3

    最后一行 rolling mean 只能看到 [0,0,0] -> mean = 0
    """
    table = table_from_l0(
        volume=[0, 0, 0, 100],
    )

    engine = FeatureL1StatEngine(window=3)
    out = engine.execute(table)
    df = out.to_pandas()

    assert df["l1_mean_w3_volume"].iloc[-1] == 0.0


# -----------------------------------------------------------------------------
# 3. window 行数语义（min_periods）
# -----------------------------------------------------------------------------
def test_l1_stat_window_min_periods_behavior():
    """
    window = 3
    前 window 行历史不足 -> 全部为 0
    """
    table = table_from_l0(
        volume=[10, 20, 30, 40],
    )

    engine = FeatureL1StatEngine(window=3)
    out = engine.execute(table)
    mean = out.to_pandas()["l1_mean_w3_volume"]

    assert mean.iloc[0] == 0.0
    assert mean.iloc[1] == 0.0
    assert mean.iloc[2] == 0.0
    assert isinstance(mean.iloc[3], float)


# -----------------------------------------------------------------------------
# 4. rolling 数值正确性（mean / sum / std）
# -----------------------------------------------------------------------------
def test_l1_stat_rolling_values_correctness():
    table = table_from_l0(
        volume=[10, 20, 30, 40],
        trade_count=[1, 2, 3, 4],
        abs_move=[1.0, 2.0, 3.0, 4.0],
    )

    engine = FeatureL1StatEngine(window=2)
    out = engine.execute(table)
    df = out.to_pandas()

    # rolling mean(volume)
    assert df["l1_mean_w2_volume"].iloc[2] == pytest.approx((10 + 20) / 2)
    assert df["l1_mean_w2_volume"].iloc[3] == pytest.approx((20 + 30) / 2)

    # rolling sum(trade_count)
    assert df["l1_sum_w2_trade_count"].iloc[2] == 1 + 2
    assert df["l1_sum_w2_trade_count"].iloc[3] == 2 + 3

    # rolling std(abs_move)
    std_expected = pd.Series([1.0, 2.0]).std()
    assert df["l1_std_w2_abs_move"].iloc[2] == pytest.approx(std_expected)


# -----------------------------------------------------------------------------
# 5. log return 语义
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# 5. log return 语义（必须存在 l0_* 列）
# -----------------------------------------------------------------------------
def test_l1_stat_log_return():
    """
    FeatureL1Stat 必须运行在 FeatureL0 之后，
    即使某个特征只使用 close，也必须存在 l0_* 列。
    """
    table = table_from_l0(
        volume=[1, 1, 1],        # dummy L0 feature
        close=[10.0, 20.0, 40.0],
    )

    engine = FeatureL1StatEngine(window=2, enable_return=True)
    out = engine.execute(table)
    df = out.to_pandas()

    assert df["l1_ret_w2_1"].iloc[0] == 0.0
    assert df["l1_ret_w2_1"].iloc[1] == pytest.approx(math.log(20.0 / 10.0))
    assert df["l1_ret_w2_1"].iloc[2] == pytest.approx(math.log(40.0 / 20.0))


# -----------------------------------------------------------------------------
# 6. ratio 特征数值稳定性（无 inf / nan）
# -----------------------------------------------------------------------------
def test_l1_stat_ratio_stability():
    table = table_from_l0(
        volume=[10, 0, 0, 50],
        range_=[1.0, 0.0, 0.0, 5.0],
    )

    engine = FeatureL1StatEngine(window=2)
    out = engine.execute(table)
    df = out.to_pandas()

    assert (df["l1_ratio_w2_volume"].replace([math.inf, -math.inf], 0.0) == df["l1_ratio_w2_volume"]).all()
    assert (df["l1_ratio_w2_range"].replace([math.inf, -math.inf], 0.0) == df["l1_ratio_w2_range"]).all()


# -----------------------------------------------------------------------------
# 7. append 模式安全性（多 window 并存）
# -----------------------------------------------------------------------------
def test_l1_stat_multiple_windows_can_coexist():
    table = table_from_l0(
        volume=[10, 20, 30, 40, 50],
    )

    out1 = FeatureL1StatEngine(window=2).execute(table)
    out2 = FeatureL1StatEngine(window=3).execute(out1)

    cols = out2.column_names

    assert "l1_mean_w2_volume" in cols
    assert "l1_mean_w3_volume" in cols
    assert "l0_volume" in cols


# -----------------------------------------------------------------------------
# 8. 空表安全性
# -----------------------------------------------------------------------------
def test_l1_stat_empty_table_is_noop():
    empty = table_from_l0(volume=[])

    engine = FeatureL1StatEngine(window=5)
    out = engine.execute(empty)

    assert out.num_rows == 0
    assert out.column_names == ["l0_volume"]
