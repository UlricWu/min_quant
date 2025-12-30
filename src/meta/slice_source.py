# src/meta/slice_source.py
from __future__ import annotations

from pathlib import Path
from typing import Iterator, Tuple

import pyarrow as pa

from src.meta.base import BaseMeta
from src.meta.slice_accessor import SliceAccessor


class SliceSource:
    """
    SliceSource（USER-FACING · FINAL）

    用户级 Facade，设计目标：
      - 用户不关心 parquet
      - 用户不关心 index
      - 用户不关心 bind
      - 只关心：slice key → slice table
    """

    # --------------------------------------------------
    def __init__(
        self,
        *,
        meta_dir: Path,
        stage: str,
        output_slot: str,
    ) -> None:
        self._meta = BaseMeta(
            meta_dir=meta_dir,
            stage=stage,
            output_slot=output_slot,
        )

        manifest = self._meta.load()

        outputs = manifest["outputs"]
        index_meta = outputs.get("index")
        if index_meta is None:
            raise RuntimeError(
                f"[SliceSource] no slice index in manifest: {self._meta.path}"
            )

        if index_meta["type"] != "symbol_slice":
            raise RuntimeError(
                f"[SliceSource] unsupported slice type: {index_meta['type']}"
            )

        self._parquet_file = Path(outputs["file"])
        self._index = {
            k: tuple(v)
            for k, v in index_meta["symbols"].items()
        }

        self._accessor = SliceAccessor.from_manifest(
            parquet_file=self._parquet_file,
            index=self._index,
        )

    # --------------------------------------------------
    def symbols(self) -> list[str]:
        return self._accessor.keys()

    # --------------------------------------------------
    def get(self, symbol: str) -> pa.Table:
        return self._accessor.get(symbol)

    # --------------------------------------------------
    def iter_tables(self) -> Iterator[Tuple[str, pa.Table]]:
        for symbol in self.symbols():
            sub = self.get(symbol)
            if sub.num_rows > 0:
                yield symbol, sub

    # --------------------------------------------------
    def __iter__(self) -> Iterator[Tuple[str, pa.Table]]:
        return self.iter_tables()
