# src/backtest/steps/report_step.py
from pathlib import Path
from src.pipeline.step import PipelineStep

from src.backtest.report.equity_curve import EquityCurveReport


class ReportStep(PipelineStep):
    """
    ReportStep（FINAL）

    职责：
      - 只读 result + metrics
      - 输出图 / md
    """

    stage = "backtest_report"
    output_slot = "report"

    def run(self, ctx):
        out_dir = Path(ctx.backtest_dir)

        EquityCurveReport(
            out_dir / "equity.png"
        ).render(ctx.result)

        report_md = f"""
# Backtest Report

- Name: {ctx.result.name}
- Date: {ctx.date}
- Symbols: {ctx.result.symbols}

## Metrics
{ctx.metrics}
"""
        (out_dir / "report.md").write_text(report_md.strip())
        return ctx
