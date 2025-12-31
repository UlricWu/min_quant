# src/meta/symbol_slice_resolver.py
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable

from src.meta.slice_source import SliceSource


class SymbolSliceResolver:
    """
    SymbolSliceResolver (FINAL / FROZEN)

    职责：
      - 给定 symbol 或 symbols
      - 在多个 SliceSource 中定位“唯一合法来源”
    """

    def __init__(
        self,
        *,
        meta_dir: Path,
        stage: str,
        output_slot: str,
    ) -> None:
        self._meta_dir = meta_dir
        self._stage = stage
        self._output_slot = output_slot

        self._sources: list[SliceSource] = []

        for exchange in ("sh_trade", "sz_trade"):
            try:
                src = SliceSource(
                    meta_dir=meta_dir,
                    stage=stage,
                    output_slot=exchange,
                )
                self._sources.append(src)
            except FileNotFoundError:
                continue

        if not self._sources:
            raise RuntimeError("No SliceSource found for resolver")

    # --------------------------------------------------
    def get(self, symbol: str):
        """
        返回：pa.Table（唯一）

        约束：
          - symbol 必须恰好存在于一个 source
        """
        result = self.get_many([symbol])
        return result[symbol]

    # --------------------------------------------------
    def get_many(self, symbols: Iterable[str]):
        """
        返回：
          Dict[symbol, pa.Table]

        冻结约束：
          - 每个 symbol 必须恰好命中一个 source
          - 0 个 or >1 个 都是错误
        """
        symbols = list(symbols)
        result: Dict[str, object] = {}

        # 预取每个 source 的 symbol 集合（性能 & 语义清晰）
        source_symbols = {
            src: set(src.symbols())
            for src in self._sources
        }

        for symbol in symbols:
            found = []

            for src, sym_set in source_symbols.items():
                if symbol in sym_set:
                    found.append(src)

            if not found:
                raise KeyError(
                    f"Symbol not found in any slice: {symbol}"
                )

            if len(found) > 1:
                raise RuntimeError(
                    f"Symbol {symbol} found in multiple slice sources"
                )

            result[symbol] = found[0].get(symbol)

        return result
