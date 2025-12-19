#!filepath: tests/normalize/test_normalize_step.py
from __future__ import annotations

from src.dataloader.pipeline.steps.normalize_step import NormalizeStep
from src.adapters.normalize_adapter import NormalizeAdapter
from src.engines.normalize_engine import NormalizeEngine
from src.dataloader.pipeline.context import PipelineContext


def test_step_skip(tmp_path):
    engine = NormalizeEngine()
    adapter = NormalizeAdapter(engine=engine, symbols=["600000"])
    step = NormalizeStep(adapter=adapter, skip_if_exists=True)

    date = "2025-01-02"
    out = tmp_path / "600000" / date / "order"
    out.mkdir(parents=True)
    (out / "Normalized.parquet").write_bytes(b"x")

    ctx = PipelineContext(date=date, symbol_dir=tmp_path, raw_dir='', parquet_dir='')
    step.run(ctx)  # 不抛异常即可
