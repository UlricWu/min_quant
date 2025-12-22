from __future__ import annotations

from pathlib import Path

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext, EngineContext
from src.engines.minute_order_agg_engine import MinuteOrderAggEngine
from src.utils.logger import logs

# from src.observability.timer import noop_timer

class MinuteOrderAggStep(PipelineStep):
    """
    MinuteOrderAggStep（无 Meta，文件存在即跳过）

    Semantics:
      symbol/{sym}/{date}/Order_Enriched.parquet
        -> symbol/{sym}/{date}/Minute_Order.parquet
    """

    def __init__(self, engine: MinuteOrderAggEngine, inst=None) -> None:
        self.engine = engine
        self.inst = inst

    def run(self, ctx: PipelineContext) -> None:
        symbol_root: Path = ctx.symbol_dir

        if not symbol_root.exists():
            logs.warning(f"[MinuteOrderAggStep] symbol_root not found: {symbol_root}")
            return

        sym_dirs = [p for p in symbol_root.iterdir() if p.is_dir()]
        count = 0


        with self.inst.timer("MinuteOrderAggStep"):
            logs.info(f"[MinuteOrderAggStep] start minute order aggregation")
            for sym_dir in sym_dirs:

                in_path = sym_dir / "orderbook_events.parquet"
                out_path = sym_dir / "minute_order.parquet"

                if not in_path.exists() or in_path.stat().st_size ==0:
                    logs.warning(f"[MinuteOrderAggStep] skipping {in_path}")
                    continue

                if out_path.exists():
                    continue

                ectx = EngineContext(
                    mode="offline",
                    input_path=in_path,
                    output_path=out_path,
                )
                self.engine.execute(ectx)
                count += 1
        logs.info(f"[MinuteOrderAggStep] process count: {count}")


        return ctx
