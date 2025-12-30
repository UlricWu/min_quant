#!filepath: src/meta/symbol_slice_source.py
from __future__ import annotations

from pathlib import Path
from typing import Iterator, Tuple, Optional

import pyarrow as pa

from src.meta.meta import BaseMeta
from src.meta.symbol_accessor import SymbolAccessor


class SymbolSliceSource:
    """
    SymbolSliceSource（FINAL / FROZEN）

    角色定位（非常重要）：
      - 用户级 Facade（User-facing）
      - 组合 BaseMeta + SymbolAccessor
      - 提供统一、干净的 per-symbol slice 访问接口

    不变原则（冻结）：
      - 不做业务计算
      - 不做 concat
      - 不写文件
      - 不关心 pipeline / backtest 语义
      - 不解析 index 结构细节

    使用者只需关心两件事：
      1) 用哪个 meta / stage
      2) 对 per-symbol table 做什么
    """

    # ------------------------------------------------------------------
    # Core constructor (EXPLICIT / CONTROLLED)
    # ------------------------------------------------------------------
    def __init__(
        self,
        *,
        meta: BaseMeta,
        input_file: Path,
        stage: str,
    ) -> None:
        """
        Parameters
        ----------
        meta:
            已构造好的 BaseMeta（控制面，必须由调用方显式持有）

        input_file:
            上游事实文件（作为 manifest 锚点）
            例如：
              - fact/sh_trade.min.parquet
              - fact/sz_trade.normalize.parquet

        stage:
            提供 symbol slice index 的 stage
            例如：
              - "normalize"
              - "min"
        """
        self._meta = meta
        self._input_file = Path(input_file)
        self._stage = stage

        self._accessor: Optional[SymbolAccessor] = None

    # ------------------------------------------------------------------
    # Convenience factory (OPTIONAL / USER-FRIENDLY)
    # ------------------------------------------------------------------
    @classmethod
    def from_meta_dir(
        cls,
        *,
        meta_dir: Path,
        stage: str,
        input_file: Path,
    ) -> "SymbolSliceSource":
        """
        Convenience constructor（便捷入口）

        适用场景：
          - Backtest
          - Research / Notebook
          - 脚本工具

        注意：
          - 这是 sugar，不是主构造路径
          - 主构造函数仍然是架构推荐入口
        """
        meta = BaseMeta(meta_dir, stage=stage)
        return cls(
            meta=meta,
            input_file=input_file,
            stage=stage,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------
    def _load_accessor(self) -> SymbolAccessor:
        """
        Lazy-load SymbolAccessor（只加载一次）
        """
        if self._accessor is None:
            manifest_path = self._meta.manifest_path(
                self._input_file,
                stage=self._stage,
            )
            self._accessor = SymbolAccessor.from_manifest(manifest_path)
        return self._accessor

    # ------------------------------------------------------------------
    # Public APIs
    # ------------------------------------------------------------------
    def iter_tables(self) -> Iterator[Tuple[str, pa.Table]]:
        """
        Iterate per-symbol tables from canonical parquet
        （直接使用 SymbolAccessor）

        Yield:
            (symbol, pa.Table)

        使用场景：
          - TradeEnrichStep
          - Normalize 之后的直接 per-symbol 计算
        """
        accessor = self._load_accessor()
        for symbol in accessor.symbols():
            sub = accessor.get(symbol)
            if sub.num_rows > 0:
                yield symbol, sub

    def bind(self, table: pa.Table) -> Iterator[Tuple[str, pa.Table]]:
        """
        Bind symbol slice index to an external row-aligned table

        Contract（冻结）：
          - table.num_rows == canonical.num_rows
          - row order 完全一致

        Yield:
            (symbol, pa.Table)

        使用场景：
          - MinuteAgg
          - FeatureBuild
          - LabelBuild
          - Backtest DataSource
        """
        accessor = self._load_accessor()
        view = accessor.bind(table)
        for symbol in view.symbols():
            sub = view.get(symbol)
            if sub.num_rows > 0:
                yield symbol, sub
