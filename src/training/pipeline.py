# src/training/pipeline.py
from __future__ import annotations

from typing import List
import pandas as pd

from src import logs
from src.utils.path import PathManager
from src.observability.instrumentation import Instrumentation
from src.pipeline.step import PipelineStep
from src.training.context import TrainingContext
from src.config.training_config import TrainingConfig


class TrainingPipeline:
    """
    TrainingPipeline（FINAL / FROZEN）

    Semantics:
    - Pipeline owns date iteration
    - Steps execute semantics
    - Pipeline collects evaluation artifacts (IC series)
    """

    def __init__(
            self,
            *,
            daily_steps: List[PipelineStep],
            final_steps: List[PipelineStep],
            pm: PathManager,
            inst: Instrumentation,
            cfg: TrainingConfig,
    ):
        self.daily_steps = daily_steps
        self.final_steps = final_steps
        self.pm = pm
        self.inst = inst
        self.cfg = cfg

        # ✅ 初始化 IC 序列（结构化）
        self.ic_series: list[dict] = []

    def run(self, run_id: str) -> TrainingContext:
        logs.info(f"[TrainingPipeline] START run_id={run_id}")

        # Context 初始化：不绑定 day
        ctx = TrainingContext(
            run_id=run_id,  # ⭐ 必须注入
            cfg=self.cfg,
            inst=self.inst,
            model_dir=self.pm.train_run_dir(run_id),
        )

        dates = self._scan_physical_trading_days()

        for i in range(len(dates) - 1):
            update_day = dates[i]

            eval_day = dates[i + 1]

            ctx.update_day = update_day
            ctx.eval_day = eval_day

            # ------------------------------
            # Run daily steps
            # ------------------------------
            for step in self.daily_steps:
                ctx = step.run(ctx)

            # ------------------------------
            # Collect IC (if exists)
            # ------------------------------
            key = f"ic@{eval_day}"
            ic = ctx.metrics.get(key)

            if ic is not None:
                ic_value = float(ic)

                logs.info(
                    f"[IC] update={update_day} "
                    f"eval={eval_day} "
                    f"ic={ic_value:.6f}"
                )

                # ✅ 结构化存储
                self.ic_series.append(
                    {
                        "update_day": update_day,
                        "eval_day": eval_day,
                        "ic": ic_value,
                    }
                )
            else:
                logs.info(
                    f"[IC] update={update_day} "
                    f"eval={eval_day.date()} "
                    f"ic=SKIPPED"
                )

        # Attach IC series to context (for report steps)
        ctx.ic_series = self.ic_series

        # ------------------------------
        # Run final steps (reports)
        # ------------------------------
        for step in self.final_steps:
            ctx = step.run(ctx)

        logs.info("[TrainingPipeline] DONE")
        return ctx

    def _scan_physical_trading_days(self) -> List[str]:
        """
        Trading day definition (PHYSICAL):

        - pm.feature_dir(date) exists
        - No exchange calendar assumption
        - No business logic here
        """
        calendar_days = pd.date_range(
            self.cfg.start_date,
            self.cfg.end_date,
            freq="D",
        ).strftime("%Y-%m-%d")

        return [d for d in calendar_days if self.pm.feature_dir(d).exists()]
