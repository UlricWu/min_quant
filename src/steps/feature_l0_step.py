from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.engines.feature_l0_engine import FeatureL0Engine
from src.utils.logger import logs
from src.meta.meta import BaseMeta, MetaResult


class FeatureL0Step(PipelineStep):
    """
    FeatureL0Step（与 MinuteTradeAggStep 完全对齐）

    Semantics:
      fact/{name}_min.parquet
        -> feature_l0/{name}_l0.parquet
    """

    def __init__(self, engine: FeatureL0Engine, inst=None) -> None:
        super().__init__(inst)  # ⭐ 关键一行
        self.engine = engine

    # --------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        fact_dir: Path = ctx.fact_dir
        feature_dir: Path = ctx.feature_dir
        meta_dir: Path = ctx.meta_dir

        stage = "l0"
        meta = BaseMeta(meta_dir, stage=stage)

        # --------------------------------------------------
        # 1. 遍历 minute fact（与 MinuteTradeAgg 完全一致）
        # --------------------------------------------------
        for input_file in fact_dir.glob("*trade_min.parquet"):
            name = input_file.stem.replace("_min", "")  # sh_trade / sz_trade

            # --------------------------------------------------
            # 2. 判定 upstream 是否变化
            # --------------------------------------------------
            if not meta.upstream_changed(input_file):
                logs.warning(f"[FeatureL0] {name} unchanged -> skip")
                continue

            # --------------------------------------------------
            # 3. 读取 minute fact
            # --------------------------------------------------
            table = pq.read_table(input_file)
            if table.num_rows == 0:
                logs.warning(f"[FeatureL0] {name} empty -> skip")
                continue

            # --------------------------------------------------
            # 4. FeatureL0 计算（业务逻辑在 Engine）
            # --------------------------------------------------
            with self.inst.timer(f"FeatureL0_{name}"):
                result_table = self.engine.execute(table)

            if result_table.num_rows == 0:
                logs.warning(f"[FeatureL0] {name} no output")
                continue

            # --------------------------------------------------
            # 5. 写出 FeatureL0 parquet
            # --------------------------------------------------
            output_file = feature_dir / f"{name}_{stage}.parquet"
            pq.write_table(result_table, output_file)

            # --------------------------------------------------
            # 6. 提交 Meta（证明结果成立）
            # --------------------------------------------------
            result = MetaResult(
                input_file=input_file,
                output_file=output_file,
                rows=result_table.num_rows,
            )
            meta.commit(result)

            logs.info(
                f"[FeatureL0] written {output_file.name} "
                f"(rows={result_table.num_rows})"
            )

        return ctx
