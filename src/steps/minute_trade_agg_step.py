#!filepath: src/steps/minute_trade_agg_step.py
from __future__ import annotations

from pathlib import Path

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext, EngineContext
from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine
from src.utils.logger import logs


class MinuteTradeAggStep(PipelineStep):
    """
    MinuteTradeAggStep（无 Meta，文件存在即跳过）

    Semantics:
      symbol/{sym}/{date}/Trade_Enriched.parquet
        -> symbol/{sym}/{date}/Minute_Trade.parquet
    """

    def __init__(self, engine: MinuteTradeAggEngine, inst=None) -> None:
        self.engine = engine
        self.inst = inst

    def run(self, ctx: PipelineContext) -> None:
        symbol_root: Path = ctx.symbol_dir

        if not symbol_root.exists():
            logs.warning(f"[MinuteTradeAggStep] symbol_root not found: {symbol_root}")
            return

        # 你的目录结构是：symbol/{date}/{sym}/xxx.parquet（从你 meta 输出看是这种）
        # 如果你实际是 symbol/{sym}/{date}/，把下面路径拼接换一下即可。
        sym_dirs = [p for p in symbol_root.iterdir() if p.is_dir()]
        logs.info(f"[MinuteTradeAggStep] process count: {len(sym_dirs)}")

        with self.inst.timer("MinuteTradeAggStep"):

            for sym_dir in sym_dirs:

                in_path = sym_dir / "trade_enriched.parquet"
                out_path = sym_dir / "minute_trade.parquet"

                if not in_path.exists():
                    continue

                if out_path.exists():
                    continue

                ectx = EngineContext(
                    mode="offline",
                    input_path=in_path,
                    output_path=out_path,
                )
                self.engine.execute(ectx)

        return ctx
