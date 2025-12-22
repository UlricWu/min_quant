from src.pipeline.context import PipelineContext
from src.pipeline.step import BasePipelineStep
from src.engines.normalize_engine import NormalizeEngine
from pathlib import Path


class NormalizeStep(BasePipelineStep):
    def __init__(self, engine: NormalizeEngine, inst=None):
        super().__init__(inst)
        self.engine = engine

    def run(self, ctx: PipelineContext) -> None:
        input_dir: Path = ctx.parquet_dir
        output_dir: Path = ctx.canonical_dir

        for file in list(input_dir.glob("*.parquet")):
            filename = file.stem
            output_file = output_dir / filename

            if output_file.exists():
                continue
            with self.inst.timer(f'NormalizeStep_{filename}'):
                self.engine.execute(
                    input_file=input_dir / file,
                    output_dir=output_dir,
                )

        return ctx
