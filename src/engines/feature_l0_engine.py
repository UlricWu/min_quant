from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


class FeatureL0Engine:
    """
    FeatureL0Engine (Frozen v1)

    输入契约：
      - 单 symbol
      - 单分钟（或 trade_agg）
      - Arrow Table
      - 不跨时间、不 rolling

    输出：
      - 行数不变
      - 仅 append / replace L0 feature 列
    """

    # --------------------------------------------------
    def execute(self, table: pa.Table) -> pa.Table:
        if table.num_rows == 0:
            return table

        self._validate_input(table)

        out = table

        # ==================================================
        # 1. 基础成交强度（Intensity）
        # ==================================================
        out = self._add_notional(out)
        out = self._add_avg_trade_size(out)

        # ==================================================
        # 2. 方向性 / 不平衡（Imbalance）
        # ==================================================
        if self._has_buy_sell(table):
            out = self._add_buy_sell_features(out)

        # ==================================================
        # 3. 价格区间 / 冲击（Range / Impact）
        # ==================================================
        out = self._add_price_impact(out)

        # ==================================================
        # 4. 统计压缩（Distribution Stabilization）
        # ==================================================
        out = self._add_log_features(out)

        return out

    # ==================================================
    # Validation
    # ==================================================
    @staticmethod
    def _validate_input(table: pa.Table) -> None:
        required = {"open", "high", "low", "close", "volume", "trade_count"}
        missing = required - set(table.column_names)
        if missing:
            raise ValueError(f"FeatureL0 missing columns: {missing}")

    # ==================================================
    # Feature blocks
    # ==================================================
    @staticmethod
    def _add_notional(table: pa.Table) -> pa.Table:
        amount = pc.multiply(
            pc.cast(table["close"], pa.float64()),
            pc.cast(table["volume"], pa.float64()),
        )
        return _append_or_replace(table, "l0_amount", amount)

    @staticmethod
    def _add_avg_trade_size(table: pa.Table) -> pa.Table:
        avg_size = pc.divide(
            pc.cast(table["volume"], pa.float64()),
            pc.cast(table["trade_count"], pa.float64()),
        )

        avg_size = pc.if_else(
            pc.greater(pc.cast(table["trade_count"], pa.float64()), 0),
            avg_size,
            0.0,
        )
        # 0.0 / 0.0  →  NaN   （不是 null）->补0

        return _append_or_replace(table, "l0_avg_trade_size", avg_size)

    @staticmethod
    def _has_buy_sell(table: pa.Table) -> bool:
        return {"buy_volume", "sell_volume"} <= set(table.column_names)

    @staticmethod
    def _add_buy_sell_features(table: pa.Table) -> pa.Table:
        buy = pc.cast(table["buy_volume"], pa.float64())
        sell = pc.cast(table["sell_volume"], pa.float64())
        total = pc.add(buy, sell)

        buy_ratio = pc.if_else(
            pc.greater(total, 0),
            pc.divide(buy, total),
            0.0,
        )

        imbalance = pc.if_else(
            pc.greater(total, 0),
            pc.divide(pc.subtract(buy, sell), total),
            0.0,
        )

        table = _append_or_replace(table, "l0_buy_ratio", buy_ratio)
        table = _append_or_replace(table, "l0_imbalance", imbalance)
        return table

    @staticmethod
    def _add_price_impact(table: pa.Table) -> pa.Table:
        range_ = pc.subtract(
            pc.cast(table["high"], pa.float64()),
            pc.cast(table["low"], pa.float64()),
        )

        abs_move = pc.abs(
            pc.subtract(
                pc.cast(table["close"], pa.float64()),
                pc.cast(table["open"], pa.float64()),
            )
        )

        table = _append_or_replace(table, "l0_range", range_)
        table = _append_or_replace(table, "l0_abs_move", abs_move)
        return table

    @staticmethod
    def _add_log_features(table: pa.Table) -> pa.Table:
        log_volume = pc.log1p(pc.cast(table["volume"], pa.float64()))
        log_trade = pc.log1p(pc.cast(table["trade_count"], pa.float64()))

        table = _append_or_replace(table, "l0_log_volume", log_volume)
        table = _append_or_replace(table, "l0_log_trade_count", log_trade)
        return table


# ======================================================
# Utility
# ======================================================
def _append_or_replace(table: pa.Table, name: str, arr: pa.Array) -> pa.Table:
    if name in table.column_names:
        idx = table.column_names.index(name)
        return table.set_column(idx, name, arr)
    return table.append_column(name, arr)
