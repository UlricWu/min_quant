# src/steps/csv_convert_step.py
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict

from src.pipeline.step import BasePipelineStep
from src.pipeline.pipeline import PipelineContext
from src.utils.logger import logs

from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind

from src.engines.convert_engine import ConvertEngine

from src.meta.meta import BaseMeta, MetaResult


class CsvConvertStep(BasePipelineStep):
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

    def __init__(self, inst=None):
        super().__init__(inst)

    # ======================================================
    # Step 入口（始终串行）
    # ======================================================
    def run(self, ctx: PipelineContext):
        raw_dir = ctx.raw_dir
        parquet_dir = ctx.parquet_dir

        zfiles = sorted(raw_dir.glob("*.7z"))
        if not zfiles:
            logs.warning("[CsvConvertStep] no .7z files found")
            return ctx

        meta = BaseMeta(ctx.meta_dir, stage="convert")

        items = []
        for zfile in zfiles:
            out_files = _build_out_files(zfile, parquet_dir)
            # --------------------------------------------------
            # Meta 判断：是否需要重新转换
            # --------------------------------------------------
            if not meta.upstream_changed(zfile):
                logs.info(
                    f"[CsvConvertStep] meta hit → skip {zfile.name}"
                )
                continue

            # 输出文件若部分缺失，也必须重跑
            if not _all_exist(out_files):
                items.append((str(zfile), str(parquet_dir)))
            else:
                logs.info(
                    f"[CsvConvertStep] outputs exist but meta mismatch → rerun {zfile.name}"
                )
                items.append((str(zfile), str(parquet_dir)))

        if not items:
            logs.info("[CsvConvertStep] nothing to convert")
            return ctx

        with self.inst.timer('CsvConvertStep'):
            # Path 不能保证在所有平台 / 所有 start method / 所有 Python 版本下是 100% 可预期
            ParallelExecutor.run(
                kind=ParallelKind.FILE,
                items=items,
                handler=_convert_one_file,
            )

        # --------------------------------------------------
        # 成功后提交 Meta（逐文件）
        # --------------------------------------------------
        for zfile in zfiles:
            if not meta.upstream_changed(zfile):
                continue

            out_files = _build_out_files(zfile, parquet_dir)
            if not _all_exist(out_files):
                continue

            # 选一个“主输出”作为 manifest 的 outputs.file
            main_output = next(iter(out_files.values()))

            meta.commit(
                MetaResult(
                    input_file=zfile,
                    output_file=main_output,
                    rows=0,  # CsvConvert 阶段不关心 rows
                )
            )

            logs.info(
                f"[CsvConvertStep] meta committed for {zfile.name}"
            )

        return ctx


def _convert_one_file(item: tuple[str, str]) -> None:
    """
    CsvConvert worker（子进程入口）

    ⚠️ 重要约束：
    - 不依赖 Step / ctx
    - 不使用全局状态
    - 每次调用创建独立 Engine
    """
    zfile, parquet_dir = item
    zpath = Path(zfile)
    parquet_dir = Path(parquet_dir)

    out_files = _build_out_files(zpath, parquet_dir)

    if _all_exist(out_files):
        logs.info(f"[CsvConvertStep] {zpath.name} exists -> skip")
        return

    logs.info(
        f"[CsvConvertWorker] pid={os.getpid()} file={Path(zfile).name}"
    )

    engine = ConvertEngine()
    engine.convert(zpath, out_files)


# ======================================================
# 纯逻辑工具函数（可复用 / 可单测）
# ======================================================

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


def _all_exist(out_files: Dict[str, Path]) -> bool:
    return all(p.exists() for p in out_files.values())
