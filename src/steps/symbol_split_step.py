#!filepath: src/steps/symbol_split_step.py
from __future__ import annotations

from typing import Any

import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.meta import MetaRegistry
from src.engines.symbol_split_engine import SymbolSplitEngine
from pathlib import Path
from src.utils.filesystem import FileSystem
from src.utils.logger import logs


class SymbolSplitStep(PipelineStep):
    """
    SymbolSplitStep（Meta-aware，冻结版）

    Semantic:
        canonical Events.parquet
            → symbol/{symbol}/{date}/Trade.parquet
    SymbolSplitStep — DAILY-CLOSED (data-driven) FINAL VERSION

    Semantics (FROZEN):

    - Meta is DATE-scoped.
    - Daily universe is defined ONLY by that day's meta.outputs.
    - First run (no meta):
        * Read canonical once
        * Discover symbols appearing on THIS date
        * Full split
        * Write meta (universe = discovered symbols)
    - Subsequent runs:
        * Universe = meta.outputs.keys()
        * If all outputs valid -> SKIP (NO canonical IO)
        * If some outputs invalid/missing -> read canonical and repair ONLY those symbols
    - Does NOT detect symbols missing due to upstream canonical issues.
    """

    def __init__(
            self,
            engine: SymbolSplitEngine,
            inst=None,
    ):
        self.engine = engine
        self.inst = inst

    # --------------------------------------------------
    def run(self, ctx):
        input_dir: Path = ctx.canonical_dir
        output_dir: Path = ctx.symbol_dir

        meta_dir: Path = ctx.meta_dir

        for file in list(input_dir.glob("*.parquet")):
            with self.inst.timer(f'SymbolSplitStep_{file.stem}'):

                meta, symbols = self._needs_split(file, meta_dir)

                if not symbols:
                    logs.warning(f"[SymbolSplitStep] skip {file.stem}")
                    continue
                # ② 读取 canonical table（一次）
                table = pq.read_table(file)
                # ③ 执行 split（纯逻辑）
                payloads = self.engine.split_many(table, symbols)

                # ④ 写文件 + 记录 meta
                meta.begin_new()

                for sym, data in payloads.items():
                    out_file = output_dir / sym / file.name.split('_')[1]
                    FileSystem.safe_write(out_file, data)
                    meta.record_output(sym, out_file)

                meta.commit()

        return ctx

    @logs.catch()
    def _needs_split(self, file: Path, meta_dir: Path) -> tuple[Any, MetaRegistry]:
        # ① 修正 step 语义：pipeline step + file
        step_key = f"{self.__class__.__name__}:{file.stem}"

        meta = MetaRegistry(
            meta_dir=meta_dir,
            step=step_key,
            input_file=file,
            engine_version="v1"
        )
        manifest = meta.load()

        # ---------------------------------------------
        # ① 决定需要 split 的 symbol
        # ---------------------------------------------
        if manifest is None or meta.is_input_changed():
            table = pq.read_table(file, columns=["symbol"])
            symbols = table["symbol"].unique().to_pylist()
        else:
            status = meta.validate_outputs()
            symbols = [k for k, ok in status.items() if not ok]
        return meta, symbols
