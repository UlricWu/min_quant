#!filepath: src/backtest/steps/alpha/engine_a_report_step.py
from __future__ import annotations

from pathlib import Path

from src import logs, PathManager
from src.backtest.context import BacktestContext
from src.pipeline.step import PipelineStep

# 如果你已有这个 report 类就复用；没有的话你可以先写个最小实现
from src.backtest.report.equity_curve import EquityCurveReport


class EngineAReportStep(PipelineStep):
    """
    EngineAReportStep（FINAL / FROZEN）

    Contract:
    - Read-only result consumption
    - Produce research artifacts (png/md)
    - Must NOT rerun backtest or resolve models
    """

    stage = "backtest_report"
    output_slot = "report"

    def __init__(self, *, inst):
        super().__init__(inst=inst)
        self.pm = PathManager()

    def run(self, ctx: BacktestContext) -> BacktestContext:
        out_dir = Path(self.pm.backtest_dir(ctx.run_id))  # 需要 PathManager 支持
        out_dir.mkdir(parents=True, exist_ok=True)

        if ctx.equity_curve is not None:
            EquityCurveReport(out_dir / "equity.png").render(ctx.equity_curve)

        report_md = f"""
# Backtest Report

- Run: {ctx.run_id}
- Dates: {len(ctx.cfg.dates)}
- Symbols: {ctx.symbols}

## Last Day
- Date: {ctx.today}

## Metrics
{ctx.metrics}
""".strip()

        (out_dir / "report.md").write_text(report_md, encoding="utf-8")
        logs.info(report_md)
        return ctx
