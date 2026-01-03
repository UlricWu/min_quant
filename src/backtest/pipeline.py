from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

from src import logs, PathManager
from src.config.backtest_config import BacktestConfig
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
    inst: object

    # runtime (per-date)
    today: str | None = None
    symbols: list[str] | None = None

    # path
    meta_dir: Path = None

    # engine outputs
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
            cfg: BacktestConfig,
    ):
        self.daily_steps = daily_steps
        self.final_steps = final_steps
        self.pm = pm
        self.inst = inst
        self.cfg = cfg

    def run(self, run_id: str) -> BacktestContext:
        logs.info(f"[BacktestPipeline] START run_id={run_id}")

        ctx = BacktestContext(cfg=self.cfg, inst=self.inst)
        ctx.symbols = list(self.cfg.symbols)

        for today in self.cfg.dates:
            # logs.info(f"[BacktestPipeline] DATE={today}")
            ctx.today = today

            ctx.meta_dir = self.pm.meta_dir(today)

            for step in self.daily_steps:
                ctx = step.run(ctx)

        for step in self.final_steps:
            ctx = step.run(ctx)

        logs.info("[BacktestPipeline] DONE")
        return ctx
