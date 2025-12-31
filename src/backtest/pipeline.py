# src/backtest/pipeline.py
from __future__ import annotations

from pathlib import Path

from src import logs
from src.observability.instrumentation import Instrumentation
from src.pipeline.step import PipelineStep
from src.utils.path import PathManager

from src.pipeline.context import BacktestContext


class BacktestPipeline:
    """
    BacktestPipeline（FINAL / FROZEN）

    语义：
      - Backtest 的 orchestration 层
      - 与 DataPipeline 同构，但语义独立
      - 只负责：
          * Context 构造
          * Step 顺序执行
          * 输出目录管理
    """

    def __init__(
        self,
        *,
        steps: list[PipelineStep],
        pm: PathManager,
        inst: Instrumentation,
    ) -> None:
        self.steps = steps
        self.pm = pm
        self.inst = inst

    # --------------------------------------------------
    def run(self, date: str):
        """
        运行一次 Backtest（一个 data_version / date）

        冻结规则：
          - date 是“数据版本标识”，不是交易日语义
          - pipeline 本身不循环 dates
        """
        logs.info(f"[BacktestPipeline] ====== START {date} ======")

        # --------------------------------------------------
        # Resolve paths
        # --------------------------------------------------
        meta_dir = self.pm.meta_dir(date)
        backtest_dir = self.pm.backtest_dir(date)

        backtest_dir.mkdir(parents=True, exist_ok=True)

        # --------------------------------------------------
        # Construct BacktestContext
        # --------------------------------------------------
        ctx = BacktestContext(
            date=date,
            meta_dir=meta_dir,
            backtest_dir=backtest_dir,
        )

        # --------------------------------------------------
        # Core loop (no timer here)
        # --------------------------------------------------
        for step in self.steps:
            logs.debug(
                f"[BacktestPipeline] running step={step.step_name}"
            )
            ctx = step.run(ctx)

        # --------------------------------------------------
        # Timeline (leaf only)
        # --------------------------------------------------
        self.inst.generate_timeline_report(date)

        logs.info(f"[BacktestPipeline] ====== DONE {date} ======")

        return ctx
