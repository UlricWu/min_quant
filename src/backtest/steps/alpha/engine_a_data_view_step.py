#!filepath: src/backtest/steps/alpha/engine_a_data_view_step.py
from __future__ import annotations

from src.pipeline.step import PipelineStep
from src import logs
from src.backtest.engines.alpha.data_view import DummyMinuteDataView


class EngineADataViewStep(PipelineStep):
    """
    解析 manifest + symbols → MarketDataView
    （MVP：dummy 实现，结构已冻结）
    """

    stage = "engine_a_data_view"

    def __init__(self, *, inst):
        self.inst = inst

    def run(self, ctx):
        logs.info(f"[EngineADataViewStep] date={ctx.date}")
        ctx.data_view = DummyMinuteDataView(
            symbols=ctx.symbols,
        )
        return ctx
