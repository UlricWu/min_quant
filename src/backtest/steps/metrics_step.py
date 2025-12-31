# src/backtest/steps/metrics_step.py
import json
from pathlib import Path
from src.pipeline.step import PipelineStep

from src.backtest.metrics.base import BasicMetrics


class MetricsStep(PipelineStep):
    """
    MetricsStep（FINAL）

    职责：
      - result → metrics.json
    """

    stage = "backtest_metrics"
    output_slot = "metrics"

    def run(self, ctx):
        metrics = BasicMetrics().compute(ctx.result)

        out_dir = Path(ctx.backtest_dir)
        (out_dir / "metrics.json").write_text(
            json.dumps(metrics, indent=2),
            encoding="utf-8",
        )

        ctx.metrics = metrics
        return ctx
