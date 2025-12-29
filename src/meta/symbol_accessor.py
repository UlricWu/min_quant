from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, Tuple, Optional

import pyarrow as pa
import pyarrow.parquet as pq


class SymbolAccessor:
    """
    SymbolAccessor（冻结版 / Slice-Index 驱动）

    单一职责：
      - 从任意 manifest 读取 canonical parquet + symbol slice index
      - 提供 0-copy 的 symbol 级访问接口

    设计铁律（冻结）：
      1. 唯一数据来源：manifest（不关心 stage）
      2. 是否可 slice 只由 outputs.index 决定
      3. 不关心 pipeline / step / engine
      4. 所有 slice 均为 O(1) + 0-copy
    """

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------
    def __init__(
            self,
            *,
            table: pa.Table,
            index: Dict[str, Tuple[int, int]],
            manifest: dict,
            manifest_path: Path,
    ):
        self._table = table
        self._index = index
        self._manifest = manifest
        self._manifest_path = manifest_path

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------
    @classmethod
    def from_manifest(cls, manifest_path: Path) -> "SymbolAccessor":
        """
        从 Normalize manifest 构造 SymbolAccessor

        Parameters
        ----------
        manifest_path : Path
            Normalize 阶段生成的 *.manifest.json
        """
        manifest_path = Path(manifest_path)
        if not manifest_path.exists():
            raise FileNotFoundError(f"Normalize manifest not found: {manifest_path}")

        with manifest_path.open("r", encoding="utf-8") as f:
            manifest = json.load(f)

        # -----------------------------
        # 校验基本结构（防止脏数据）
        # -----------------------------
        # if manifest.get("stage") != "normalize":
        #     raise ValueError(
        #         f"Manifest stage must be 'normalize', got {manifest.get('stage')}"
        #     )

        outputs = manifest.get("outputs")
        if not outputs or "file" not in outputs:
            raise ValueError("Invalid normalize manifest: missing outputs.file")

        parquet_file = outputs.get("file")
        if not parquet_file:
            raise ValueError("Invalid normalize manifest: missing outputs.file")

        index_meta = outputs.get("index")
        if not isinstance(index_meta, dict):
            raise ValueError("Invalid normalize manifest: missing outputs.index")

        if index_meta.get("type") != "symbol_slice":
            raise ValueError(
                f"Unsupported index type: {index_meta.get('type')}"
            )

        symbols_meta = index_meta.get("symbols")
        if not isinstance(symbols_meta, dict):
            raise ValueError("Invalid normalize manifest: index.symbols missing")

        # -----------------------------
        # 读取 canonical parquet
        # -----------------------------
        parquet_path = Path(outputs["file"])
        if not parquet_path.exists():
            raise FileNotFoundError(f"Canonical parquet not found: {parquet_path}")

        table = pq.read_table(parquet_path)

        # -----------------------------
        # 构造 symbol slice index
        # -----------------------------
        index: Dict[str, Tuple[int, int]] = {}

        for symbol, slice_def in symbols_meta.items():
            if (
                    not isinstance(slice_def, (list, tuple))
                    or len(slice_def) != 2
            ):
                raise ValueError(
                    f"Invalid slice for symbol {symbol}: {slice_def}"
                )

            start, length = int(slice_def[0]), int(slice_def[1])
            index[symbol] = (start, length)

        return cls(
            table=table,
            index=index,
            manifest=manifest,
            manifest_path=manifest_path,
        )

    # ------------------------------------------------------------------
    # Core Access API
    # ------------------------------------------------------------------
    def get(self, symbol: str) -> pa.Table:
        """
        获取某个 symbol 的 Arrow Table（0-copy slice）

        - 若 symbol 不存在，返回空 table（schema 保持一致）
        """
        if symbol not in self._index:
            # 返回 0 行 slice，保持 schema
            return self._table.slice(0, 0)

        start, length = self._index[symbol]
        return self._table.slice(start, length)

    def symbols(self) -> Iterable[str]:
        """返回所有可用 symbol"""
        return self._index.keys()

    # ------------------------------------------------------------------
    # Introspection / Metadata
    # ------------------------------------------------------------------
    @property
    def table(self) -> pa.Table:
        """返回完整 canonical table（谨慎使用）"""
        return self._table

    @property
    def schema(self) -> pa.Schema:
        return self._table.schema

    @property
    def sorted_by(self) -> Tuple[str, ...]:
        """Normalize 阶段声明的排序键"""
        return tuple(self._manifest.get("outputs", {}).get("sorted_by", []))

    @property
    def rows(self) -> int:
        return int(self._manifest.get("outputs", {}).get("rows", self._table.num_rows))

    @property
    def manifest_path(self) -> Path:
        return self._manifest_path

    # ------------------------------------------------------------------
    # Optional helpers（不影响主语义）
    # ------------------------------------------------------------------
    def has_symbol(self, symbol: str) -> bool:
        return symbol in self._index

    def symbol_size(self, symbol: str) -> int:
        """返回某个 symbol 的行数（不存在返回 0）"""
        if symbol not in self._index:
            return 0
        _, length = self._index[symbol]
        return length

    def head(self, symbol: str, n: int = 5) -> pa.Table:
        """调试用：返回某个 symbol 的前 n 行"""
        table = self.get(symbol)
        return table.slice(0, min(n, table.num_rows))

    def bind(self, table: pa.Table) -> "SymbolTableView":
        """
        将 normalize 的 symbol slice index
        绑定到一张 row-wise 对齐的外部 table。

        Contract（冻结）：
          - table.num_rows == canonical.num_rows
          - row order 完全一致
        """
        if table.num_rows != self._table.num_rows:
            raise ValueError(
                "Cannot bind table with different row count "
                f"(canonical={self._table.num_rows}, given={table.num_rows})"
            )

        return SymbolTableView(
            table=table,
            index=self._index,
        )


class SymbolTableView:
    """
    SymbolTableView（冻结版）

    语义：
      - 使用 SymbolAccessor 提供的 slice index
      - 但数据来源是外部 table（row-aligned）
    """

    def __init__(
            self,
            *,
            table: pa.Table,
            index: Dict[str, Tuple[int, int]],
    ):
        self._table = table
        self._index = index

    def symbols(self) -> Iterable[str]:
        return self._index.keys()

    def get(self, symbol: str) -> pa.Table:
        if symbol not in self._index:
            return self._table.slice(0, 0)

        start, length = self._index[symbol]
        return self._table.slice(start, length)
