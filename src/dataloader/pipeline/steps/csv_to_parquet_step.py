#!filepath: src/dataloader/pipeline/steps/csv_to_parquet_step.py
from __future__ import annotations
from src.dataloader.pipeline.context import PipelineContext
from src.dataloader.pipeline.step import PipelineStep
from src import logs


class CsvToParquetStep(PipelineStep):

    def __init__(self, converter):
        self.converter = converter

    def _detect_type(self, name: str) -> str:
        lower = name.lower()
        if lower.startswith("sh_stock_ordertrade"):
            return "SH_MIXED"
        if lower.startswith("sh_order"): return "SH_ORDER"
        if lower.startswith("sh_trade"): return "SH_TRADE"
        if lower.startswith("sz_order"): return "SZ_ORDER"
        if lower.startswith("sz_trade"): return "SZ_TRADE"
        raise RuntimeError(f"无法识别类型：{name}")

    def run(self, ctx: PipelineContext) -> PipelineContext:
        logs.info("[Step] CsvToParquetStep")

        for zfile in ctx.raw_dir.glob("*.7z"):
            stem = zfile.stem.replace(".csv", "")
            file_type = self._detect_type(zfile.name)

            # skip logic
            if file_type == "SH_MIXED":
                if (ctx.parquet_dir / "SH_Order.parquet").exists() and \
                   (ctx.parquet_dir / "SH_Trade.parquet").exists():
                    continue
            else:
                out = ctx.parquet_dir / f"{stem}.parquet"
                if out.exists():
                    continue

            logs.info(f"→ convert {zfile.name}")
            self.converter.convert(zfile, ctx.parquet_dir, file_type)

        return ctx
