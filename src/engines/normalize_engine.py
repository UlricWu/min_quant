#!filepath: src/engines/normalize_engine.py
from __future__ import annotations

from functools import reduce

import pyarrow as pa
import pyarrow.compute as pc


class NormalizeEngine:
    """
    NormalizeEngine（冻结版）

    语义：
      - 对【单个 batch 的 Arrow Table】进行 canonical normalize
      - 使其满足“可全量拼接、可全局排序”的前置条件

    输入：
      - pa.Table（单 batch，已 parse）

    输出：
      - pa.Table（canonical 化后的单 batch）

    冻结约束（非常重要）：
      - ❌ 不 concat
      - ❌ 不感知 batch
      - ❌ 不 build index
      - ❌ 不 I/O
      - ❌ 不写 meta
      - ✅ 只处理一个 table
    """

    # --------------------------------------------------
    def execute(self, table: pa.Table) -> pa.Table:
        """
        Normalize 单 batch Arrow Table
        """
        if table is None or table.num_rows == 0:
            return table

        self._validate_required_columns(table)
        table = self.filter_a_share_arrow(table)

        # --------------------------------------------------
        # canonicalize symbol type (VERY IMPORTANT)
        # --------------------------------------------------
        sym = table["symbol"]
        if pa.types.is_dictionary(sym.type):
            table = table.set_column(
                table.column_names.index("symbol"),
                "symbol",
                pc.cast(sym, pa.string()),
            )

        # --------------------------------------------------
        # canonical sort（单 batch 内排序）
        # --------------------------------------------------
        sort_indices = pc.sort_indices(
            table,
            sort_keys=[
                ("symbol", "ascending"),
                ("ts", "ascending"),
            ],
        )
        table = table.take(sort_indices)

        return table

    # ==================================================
    # Validation（最小冻结契约）
    # ==================================================
    @staticmethod
    def _validate_required_columns(table: pa.Table) -> None:
        """
        NormalizeEngine 的最小输入契约
        """
        missing = [c for c in ("symbol", "ts") if c not in table.column_names]
        if missing:
            raise ValueError(
                f"[NormalizeEngine] missing required columns: {missing}, "
                f"have={table.column_names}"
            )

        sym = table["symbol"]
        if not (
            pa.types.is_string(sym.type)
            or pa.types.is_dictionary(sym.type)
        ):
            raise TypeError(
                f"[NormalizeEngine] column 'symbol' must be string/dictionary, "
                f"got {sym.type}"
            )

        ts = table["ts"]
        if not (
            pa.types.is_integer(ts.type)
            or pa.types.is_timestamp(ts.type)
        ):
            raise TypeError(
                f"[NormalizeEngine] column 'ts' must be integer/timestamp, "
                f"got {ts.type}"
            )

    # ==================================================
    # Optional pure helpers（仍然是单 batch）
    # ==================================================
    @staticmethod
    def filter_a_share_arrow(table: pa.Table) -> pa.Table:
        """
        A 股过滤（纯函数 / 单 batch）

        说明：
          - 可被 Step 按需调用
          - 不参与 canonical 的默认流程
        """
        if table is None or table.num_rows == 0:
            return table

        if "symbol" not in table.column_names:
            raise ValueError(
                "[NormalizeEngine] missing column: SecurityID "
                "(required for A-share filter)"
            )

        symbol = pc.cast(table["symbol"], pa.string())

        prefixes = [
            "60", "688",   # 沪市
            "00", "300",   # 深市
        ]

        masks = [pc.starts_with(symbol, p) for p in prefixes]
        mask = reduce(pc.or_, masks)

        return table.filter(mask)
