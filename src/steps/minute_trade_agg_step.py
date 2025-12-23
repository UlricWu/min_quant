#!filepath: src/steps/minute_trade_agg_step.py
from __future__ import annotations

from pathlib import Path

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext, EngineContext
from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine
from src.utils.logger import logs
from src.meta.meta import BaseMeta, MetaResult
import pyarrow as pa
import pyarrow.parquet as pq
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
        fact_dir: Path = ctx.fact_dir
        meta_dir: Path = ctx.meta_dir

        stage = "min"
        meta = BaseMeta(meta_dir, stage=stage)

        for input_file in fact_dir.glob("*_enriched.parquet"):
            name = input_file.stem.replace("_enriched", "")  # sh_trade / sz_trade
            # --------------------------------------------------
            # 1. 判定 enriched 事实是否仍然成立
            # --------------------------------------------------
            if not meta.upstream_changed(input_file):
                logs.warning(f"[MinuteTradeAgg] {name} unchanged -> skip")
                continue

            # --------------------------------------------------
            # 2. 读取 enrich fact
            # --------------------------------------------------
            table = pq.read_table(input_file)
            if table.num_rows == 0:
                logs.warning(f"[MinuteTradeAgg] {name} empty -> skip")
                continue
            # --------------------------------------------------
            # 3. 聚合（业务逻辑在 Engine）
            # --------------------------------------------------
            with self.inst.timer(f"MinuteTradeAgg_{name}"):
                result_table = self.engine.execute(table)

            if result_table.num_rows == 0:
                logs.warning(f"[MinuteTradeAgg] {name} no minute data")
                continue

            # --------------------------------------------------
            # 4. 写出 minute fact
            # --------------------------------------------------
            output_file = fact_dir / f"{name}_{stage}.parquet"
            pq.write_table(result_table, output_file)

            # --------------------------------------------------
            # 5. 提交 Meta（证明结果成立）
            # --------------------------------------------------
            result = MetaResult(
                input_file=input_file,
                output_file=output_file,
                rows=result_table.num_rows,
            )
            meta.commit(result)

            logs.info(
                f"[MinuteTradeAgg] written {output_file.name} "
                f"(rows={result_table.num_rows})"
            )

        return ctx