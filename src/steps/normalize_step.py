from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json

from src import logs
from src.pipeline.context import PipelineContext
from src.pipeline.step import BasePipelineStep
from src.engines.normalize_engine import NormalizeEngine

from src.meta.meta import BaseMeta


class NormalizeStep(BasePipelineStep):
    def __init__(self, engine: NormalizeEngine, inst=None):
        super().__init__(inst)
        self.engine = engine

    def run(self, ctx: PipelineContext) -> PipelineContext:
        meta = BaseMeta(ctx.meta_dir, stage='normalize')

        for input_file in ctx.parquet_dir.glob("*.parquet"):
            if not meta.upstream_changed(input_file):
                logs.warning(f"[NormalizeStep] {input_file.name} unchanged -> skip")
                continue

            with self.inst.timer(f'[NormalizeStep] {input_file.name}'):

                result = self.engine.execute(
                    input_file=input_file,
                    output_dir=ctx.canonical_dir,
                )
                meta.commit(result)
        return ctx
