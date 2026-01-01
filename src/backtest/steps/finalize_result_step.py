#!filepath: src/backtest/steps/result_step.py
from __future__ import annotations

from src.pipeline.step import PipelineStep
from src import logs

from src.backtest.result import BacktestResult


class FinalizeResultStep(PipelineStep):
    """
    FinalizeResultStep（FINAL / FROZEN）

    语义：
      - 在 backtest 全部 date replay 完成后执行一次
      - 汇总 recorder / portfolio
      - 生成 BacktestResult

    冻结规则：
      - 不读取 ctx.date（无 per-day 语义）
      - 不修改 portfolio / recorder
      - 只生成 result
    """

    stage = "backtest_finalize_result"

    def run(self, ctx):
        if ctx.recorder is None or ctx.portfolio is None:
            raise RuntimeError(
                "[FinalizeResult] recorder / portfolio not initialized"
            )

        with self.timed():
            logs.info("[FinalizeResult] building backtest result")

            result = BacktestResult(
                run_id=ctx.run_id,
                portfolio=ctx.portfolio,
                trades=ctx.recorder.trades(),
                equity_curve=ctx.recorder.equity_curve(),
                positions=ctx.recorder.positions(),
            )

            ctx.result = result

        return ctx
