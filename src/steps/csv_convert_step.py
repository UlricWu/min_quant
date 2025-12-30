# src/steps/csv_convert_step.py
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from src.pipeline.step import PipelineStep
from src.utils.logger import logs

from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind

from src.engines.convert_engine import ConvertEngine

from src.meta.base import BaseMeta, MetaOutput

# from src.meta.public import should_run
# -----------------------------------------------------------------------------
# unit spec (worker contract)
# -----------------------------------------------------------------------------
from src.pipeline.context import PipelineContext

from src.pipeline.context import EngineContext


def _convert_one_unit(spec: PipelineContext) -> PipelineContext:
    """
    CsvConvert worker（进程安全）

    约束：
      - 不依赖 ctx
      - 不使用 BaseMeta
      - 不 commit meta
      - 只做纯计算
        CsvConvert worker（子进程入口）

    ⚠️ 重要约束：
    - 不依赖 Step / ctx
    - 不使用全局状态
    - 每次调用创建独立 Engine
    """
    assert hasattr(spec, "input_file"), spec
    logs.info(
        f"[CsvConvertWorker] pid={os.getpid()} file={spec.input_file.name}"
    )
    engine = ConvertEngine()
    engine.convert(spec)
    return spec


class CsvConvertStep(PipelineStep):
    """
    CsvConvertStep（ProcessPoolExecutor 版 · 冻结）

    并行粒度：
        - 文件级（*.7z）

    并行机制：
        - ProcessPoolExecutor（经 ParallelExecutor 统一管理）

    Engine 生命周期：
        - worker 内创建
        - 不跨进程复用
    """
    stage = 'CsvConvert'

    def __init__(self, stage: str, inst=None, max_workers: int = 2):
        super().__init__(inst)
        self.stage = stage
        self.max_workers = max_workers

    # ======================================================
    # Step 入口（始终串行）
    # ======================================================
    def run(self, ctx: PipelineContext):
        runnable_units = self._build_units(ctx)
        if not runnable_units:
            logs.info(f"[{self.stage}] all units up-to-date")
            return ctx

        specs = list(runnable_units.values())

        # --------------------------------------------------
        # 1. 并行执行（worker 不触碰 meta）
        # --------------------------------------------------
        with self.inst.timer(f"{self.stage} parallel"):
            # Path 不能保证在所有平台 / 所有 start method / 所有 Python 版本下是 100% 可预期
            done_specs = ParallelExecutor.run(
                kind=ParallelKind.FILE,
                items=specs,  # ← 注意：不是 str，而是 ConvertUnitSpec
                handler=_convert_one_unit,
                max_workers=self.max_workers
            )

        # --------------------------------------------------
        # 2. 主进程 commit meta（严格串行）
        # --------------------------------------------------
        for spec in done_specs:
            meta = BaseMeta(
                meta_dir=ctx.meta_dir,
                stage=self.stage,
                output_slot=spec.key,
            )

            if not spec.output_file.exists():
                raise RuntimeError(
                    f"[{self.stage}] output missing after convert: {spec.output_file}"
                )

            meta.commit(
                MetaOutput(
                    input_file=spec.input_file,
                    output_file=spec.output_file,
                    rows=-1,
                )
            )

            logs.info(f"[{self.stage}] meta committed → {meta.name}")

        return ctx

    def _build_units(self, ctx: PipelineContext) -> Dict[str, EngineContext]:
        units: Dict[str, EngineContext] = {}
        for zfile in list(ctx.raw_dir.glob("*.7z")):
            out_map = _build_out_files(zfile=zfile, parquet_dir=ctx.parquet_dir)

            for unit, dst in out_map.items():
                if unit in units:
                    raise RuntimeError(f"[{self.stage}] duplicate unit detected: {unit}")
                meta = BaseMeta(
                    meta_dir=ctx.meta_dir,
                    stage=self.stage,
                    output_slot=unit,
                )
                if not meta.upstream_changed():
                    logs.warning(
                        f"[{self.stage}] meta hit → skip {zfile.name}"
                    )
                    continue
                units[unit] = EngineContext(
                    key=unit,
                    input_file=zfile,
                    output_file=dst,
                    mode='full' if len(out_map) == 1 else 'split'
                )

        return units


# # ======================================================
# # 纯逻辑工具函数（可复用 / 可单测）
# # ======================================================
#
def _detect_type(filename: str) -> str:
    lower = filename.lower()

    if lower.startswith("sh_stock_ordertrade"):
        return "SH_MIXED"

    if lower.startswith("sh_order"):
        return "SH_ORDER"
    if lower.startswith("sh_trade"):
        return "SH_TRADE"

    if lower.startswith("sz_order"):
        return "SZ_ORDER"
    if lower.startswith("sz_trade"):
        return "SZ_TRADE"

    raise RuntimeError(f"无法识别文件类型: {filename}")


def _build_out_files(zfile: Path, parquet_dir: Path) -> Dict[str, Path]:
    file_type = _detect_type(zfile.stem)

    if file_type == "SH_MIXED":
        return {
            "sh_order": parquet_dir / "sh_order.parquet",
            "sh_trade": parquet_dir / "sh_trade.parquet",
        }

    stem = zfile.stem.replace(".csv", "")
    return {
        stem.lower(): parquet_dir / f"{stem.lower()}.parquet"
    }


def _detect_type(filename: str) -> str:
    lower = filename.lower()

    if lower.startswith("sh_stock_ordertrade"):
        return "SH_MIXED"

    if lower.startswith("sh_order"):
        return "SH_ORDER"
    if lower.startswith("sh_trade"):
        return "SH_TRADE"

    if lower.startswith("sz_order"):
        return "SZ_ORDER"
    if lower.startswith("sz_trade"):
        return "SZ_TRADE"

    raise RuntimeError(f"无法识别文件类型: {filename}")
