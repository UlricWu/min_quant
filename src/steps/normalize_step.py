# src/pipeline/steps/normalize_step.py
from __future__ import annotations

from pathlib import Path

from src import logs
from src.pipeline.context import PipelineContext
from src.pipeline.step import PipelineStep
from src.pipeline.parallel.executor import ParallelExecutor
from src.pipeline.parallel.types import ParallelKind
from src.meta.meta import BaseMeta
from src.engines.normalize_engine import NormalizeEngine


def normalize_one(
        *,
        input_file: Path,
        output_dir: Path,
) -> dict:
    """
    Normalize worker（进程安全）

    约束：
    - top-level function
    - 不依赖 ctx / self
    - 不写 meta
    - 不写日志
    """
    engine = NormalizeEngine()
    return engine.execute(
        input_file=input_file,
        output_dir=output_dir,
    )


class NormalizeStep(PipelineStep):
    """
    NormalizeStep（ProcessPool 并行版）

    并行模型：
        - 主进程：meta 判定 + commit
        - 子进程：纯 normalize
    """

    def __init__(
            self,
            inst=None,
            max_workers: int | None = 2,
    ):
        super().__init__(inst)
        self.max_workers = max_workers

    def run(self, ctx: PipelineContext) -> PipelineContext:
        meta = BaseMeta(ctx.meta_dir, stage="normalize")

        # --------------------------------------------------
        # ① master：筛选需要处理的文件
        # --------------------------------------------------
        inputs: list[Path] = []
        for input_file in ctx.parquet_dir.glob("*.parquet"):
            if not meta.upstream_changed(input_file):
                logs.info(
                    f"[NormalizeStep] {input_file.name} unchanged -> skip"
                )
                continue
            inputs.append(input_file)

        if not inputs:
            logs.info("[NormalizeStep] no files to normalize")
            return ctx

        # --------------------------------------------------
        # ② master：准备并行参数（纯数据）
        # --------------------------------------------------
        items = [
            {
                "input_file": path,
                "output_dir": ctx.canonical_dir,
            }
            for path in inputs
        ]

        # --------------------------------------------------
        # ③ 并行执行（无闭包）
        # --------------------------------------------------
        with self.inst.timer(
                f"[NormalizeStep] parallel normalize | files={len(items)}"
        ):

            results = ParallelExecutor.run(
                kind=ParallelKind.FILE,
                items=items,
                handler=_normalize_handler,
                max_workers=self.max_workers,
            )

        # --------------------------------------------------
        # ④ master：统一 commit meta
        # --------------------------------------------------
        outputs: list[str] = []
        for result in results or []:
            meta.commit(result)
            name = result.output_file.stem  # e.g. "sz_trade"
            outputs.append(name)
        if results is None:
            raise RuntimeError(
                "[NormalizeStep] ParallelExecutor returned None"
            )

        logs.info(f'[NormalizeStep] files {outputs}  to normalize')

        # 🔒 唯一可信输入集
        ctx.normalize_outputs = outputs

        return ctx


def _normalize_handler(payload: dict) -> dict:
    """
    ProcessPool handler（top-level）
    """
    return normalize_one(
        input_file=payload["input_file"],
        output_dir=payload["output_dir"],
    )
