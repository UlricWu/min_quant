# src/backtest/steps/result_step.py
import json
from pathlib import Path
from src.pipeline.step import PipelineStep

from src.backtest.result import BacktestResult


class ResultStep(PipelineStep):
    """
    ResultStep（FINAL）

    职责：
      - portfolio → BacktestResult
      - 写 result.json
    """

    stage = "backtest_result"
    output_slot = "result"

    def __init__(self, *, backtest_cfg, inst=None):
        super().__init__(inst=inst)
        self._bt = backtest_cfg

    def run(self, ctx):
        p = ctx.portfolio

        result = BacktestResult(
            name=self._bt.name,
            level=self._bt.level,
            dates=[ctx.date],
            symbols=self._bt.symbols,
            replay=self._bt.replay,
            strategy=self._bt.strategy,

            start_ts=p.timestamps[0],
            end_ts=p.timestamps[-1],
            n_events=len(p.timestamps),
            n_signals=p.n_signals,

            timestamps=p.timestamps,
            equity_curve=p.equity_curve,
        )

        out_dir = Path(ctx.backtest_dir)
        out_dir.mkdir(parents=True, exist_ok=True)

        (out_dir / "result.json").write_text(
            json.dumps(result.__dict__, indent=2),
            encoding="utf-8",
        )

        ctx.result = result
        return ctx
