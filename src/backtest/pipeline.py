from __future__ import annotations

from dataclasses import dataclass
from typing import List

from src import logs, PathManager
from src.observability.instrumentation import Instrumentation
from src.pipeline.step import PipelineStep


@dataclass
class BacktestContext:
    """
    BacktestContext（冻结）

    原则：
    - cfg / pm / inst 由 Pipeline 注入
    - date / symbols 在 Pipeline 层绑定
    - Step 只读 + 写自己负责的字段
    """
    cfg: object
    pm: object
    inst: object

    # runtime (per-date)
    date: str | None = None
    symbols: list[str] | None = None

    # engine outputs
    data_view: object | None = None
    portfolio: object | None = None
    equity_curve: object | None = None
    report: object | None = None


class BacktestPipeline:
    """
    BacktestPipeline（FINAL）

    语义：
    - Pipeline 层负责：
        * cfg.dates 循环
        * ctx.date / ctx.symbols 绑定
    - Step 层负责：
        * 语义执行
    """

    def __init__(
            self,
            *,
            daily_steps: List[PipelineStep],
            final_steps: List[PipelineStep],
            pm: PathManager,
            inst: Instrumentation,
            cfg,
    ):
        self.daily_steps = daily_steps
        self.final_steps = final_steps
        self.pm = pm
        self.inst = inst
        self.cfg = cfg

    def run(self, run_id: str) -> BacktestContext:
        logs.info(f"[BacktestPipeline] START run_id={run_id}")

        ctx = BacktestContext(cfg=self.cfg, pm=self.pm, inst=self.inst)

        for date in self.cfg.dates:
            logs.info(f"[BacktestPipeline] DATE={date}")
            ctx.date = date
            ctx.symbols = list(self.cfg.symbols)

            for step in self.daily_steps:
                ctx = step.run(ctx)

        for step in self.final_steps:
            ctx = step.run(ctx)

        logs.info("[BacktestPipeline] DONE")
        return ctx
