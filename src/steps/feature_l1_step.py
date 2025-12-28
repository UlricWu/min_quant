from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.engines.feature_l1_engine import FeatureL1Engine
from src.meta.meta import BaseMeta, MetaResult
from src.utils.logger import logs


class FeatureL1Step(PipelineStep):
    """
    FeatureL1Step (Frozen v1.1, A1 append mode)

    Semantics:
      - 若 feature_l1/{name}_l1.parquet 存在：
          读取它并追加当前 window 的 L1 特征
      - 否则：
          从 feature_l0/{name}_l0.parquet 生成

    第一次运行（window=20）:
  feature_l0/sh_trade_l0.parquet
    -> feature_l1/sh_trade_l1.parquet
       (新增 l1_z_w20_*)

    第二次运行（window=60）:
      feature_l1/sh_trade_l1.parquet
        -> feature_l1/sh_trade_l1.parquet
           (追加 l1_z_w60_*)

    """

    def __init__(self, engine: FeatureL1Engine, inst=None) -> None:
        self.engine = engine
        self.inst = inst

    # --------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        l0_dir: Path = ctx.feature_dir
        l1_dir: Path = ctx.feature_dir
        meta_dir: Path = ctx.meta_dir

        # 🔒 stage 显式包含 window，避免 Meta 冲突
        stage = f"l1_w{self.engine.window}"
        meta = BaseMeta(meta_dir, stage=stage)

        for l0_file in l0_dir.glob("*_l0.parquet"):
            name = l0_file.stem.replace("_l0", "")
            output_file = l1_dir / f"{name}_l1.parquet"

            # --------------------------------------------------
            # 1. upstream 判定（window-aware）
            # --------------------------------------------------
            if not meta.upstream_changed(l0_file):
                logs.warning(
                    f"[FeatureL1 w={self.engine.window}] {name} unchanged -> skip"
                )
                continue

            # --------------------------------------------------
            # 2. 选择 base table
            # --------------------------------------------------
            if output_file.exists():
                base_table = pq.read_table(output_file)
                logs.info(
                    f"[FeatureL1 w={self.engine.window}] "
                    f"{name} append to existing l1"
                )
            else:
                base_table = pq.read_table(l0_file)
                logs.info(
                    f"[FeatureL1 w={self.engine.window}] "
                    f"{name} build from l0"
                )

            if base_table.num_rows == 0:
                logs.warning(f"[FeatureL1] {name} empty -> skip")
                continue

            # --------------------------------------------------
            # 3. FeatureL1 计算
            # --------------------------------------------------
            with self.inst.timer(f"FeatureL1_{name}_w{self.engine.window}"):
                result = self.engine.execute(base_table)

            # --------------------------------------------------
            # 4. 写回 parquet（覆盖写，schema 扩展）
            # --------------------------------------------------
            pq.write_table(result, output_file)

            # --------------------------------------------------
            # 5. Meta commit
            # --------------------------------------------------
            meta.commit(
                MetaResult(
                    input_file=l0_file,
                    output_file=output_file,
                    rows=result.num_rows,
                )
            )

            logs.info(
                f"[FeatureL1 w={self.engine.window}] "
                f"written {output_file.name} (rows={result.num_rows})"
            )

        return ctx
