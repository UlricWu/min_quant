#!filepath: src/backtest/steps/alpha/engine_a_report_step.py
from __future__ import annotations

from src.pipeline.step import PipelineStep
from src import logs


class EngineAReportStep(PipelineStep):
    """
    Engine A Report（MVP）
    """

    stage = "engine_a_report"

    def __init__(self, *, inst):
        self.inst = inst

    def run(self, ctx):
        logs.info(
            f"[EngineAReportStep] "
            f"final_cash={ctx.portfolio.cash} "
            f"positions={ctx.portfolio.positions}"
        )
        ctx.report = {
            "cash": ctx.portfolio.cash,
            "positions": ctx.portfolio.positions,
        }
        return ctx
