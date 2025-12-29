# src/pipeline/steps/base_step.py
from __future__ import annotations

from abc import ABC, abstractmethod
from src import logs
from src.pipeline.context import PipelineContext
from src.meta.meta import BaseMeta
from contextlib import nullcontext

class BasePipelineStep(ABC):
    """
    SerialFileStep（冻结）

    定位：
      - 串行执行
      - 输入粒度 = 文件
      - 一对一映射（upstream output → downstream output）
      - 无 fan-out / fan-in
      - 无并行

    适用场景：
      - Convert / Copy / Validate
      - Schema check
      - 简单文件级 transform

    不适用：
      - Normalize（batch + index）
      - MinuteAgg（symbol fan-out）
      - FeatureStep（未来 window / rolling）
      - 任何需要并行的 Step

    设计模式：
      - Template Method
      - 强约束，弱扩展
    """

    stage: str                # e.g. "normalize"
    upstream_stage: str       # e.g. "parquet"

    def __init__(self, engine, inst=None):
        self.engine = engine
        self.inst = inst

    # --------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        # ① 读取唯一可信输入
        attr = f"{self.upstream_stage}_outputs"
        inputs = getattr(ctx, attr, [])
        if not inputs:
            logs.info(f"[{self.__class__.__name__}] no inputs")
            return ctx

        meta = BaseMeta(ctx.meta_dir, stage=self.stage)
        outputs = []

        # ② 唯一允许的 for-loop
        for item in inputs:
            input_file = self.resolve_input(ctx, item)

            if not meta.upstream_changed(input_file):
                logs.info(
                    f"[{self.__class__.__name__}] {item} unchanged -> skip"
                )
                continue

            with self.inst.timer(f"{self.__class__.__name__}_{item}"):
                result = self.execute(item, input_file)

            output_file = self.write(ctx, item, result)

            meta.commit(
                input_file=input_file,
                output_file=output_file,
                rows=getattr(result, "num_rows", None),
            )

            outputs.append(self.logical_key(item, result))

        # ③ 发布 outputs
        if not outputs:
            logs.warning(f"[{self.__class__.__name__}] outputs empty")

        setattr(ctx, f"{self.stage}_outputs", outputs)
        return ctx

    # --------------------------------------------------
    # hook methods（子类只实现这些）
    # --------------------------------------------------
    @abstractmethod
    def resolve_input(self, ctx: PipelineContext, item):
        ...

    @abstractmethod
    def execute(self, item, input_file):
        ...

    @abstractmethod
    def write(self, ctx: PipelineContext, item, result):
        ...

    def logical_key(self, item, result):
        return item
#
# # src/pipeline/steps/trade_enrich_step.py
# from pathlib import Path
# import pyarrow as pa
# import pyarrow.parquet as pq
#
# from src.pipeline.steps.base_step import BasePipelineStep
#
#
# class TradeEnrichStep(BasePipelineStep):
#     stage = "enriched"
#     upstream_stage = "normalize"
#
#     def resolve_input(self, ctx, name: str) -> Path:
#         return ctx.canonical_dir / f"{name}.parquet"
#
#     def execute(self, name, input_file: Path):
#         return self.engine.execute(input_file)
#
#     def write(self, ctx, name, table: pa.Table) -> Path:
#         output = ctx.fact_dir / f"{name}_enriched.parquet"
#         pq.write_table(table, output)
#         return output