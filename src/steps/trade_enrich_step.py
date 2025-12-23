#!filepath: src/pipeline/steps/trade_enrich_step.py
from __future__ import annotations

from pathlib import Path
from typing import List

from py7zr.helpers import canonical_path

from src import logs
import pyarrow as pa
import pyarrow.parquet as pq
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.pipeline.context import EngineContext

from src.meta.symbol_accessor import SymbolAccessor
from src.engines.trade_enrich_engine import TradeEnrichEngine
from src.meta.meta import BaseMeta
from src.pipeline.context import PipelineContext
from src.meta.meta import MetaResult

class TradeEnrichStep:
    """
    TradeEnrichStep（冻结 MVP 版）

    输入：
      - Normalize 阶段产出的 manifest + canonical parquet

    输出：
      - enrich 后的 parquet（按 symbol / 按天）

    职责：
      - orchestration only

    职责：
      - orchestration only
      - 不做业务计算
      - 不关心 index 细节
    """

    def __init__(
            self,
            engine: TradeEnrichEngine,
            inst=None,
    ):
        self.engine = engine
        self.inst = inst

    # --------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        """
        TradeEnrich 主流程

        约定：
          - Normalize 已完成
          - Normalize Meta 存在
        """
        input_dir: Path = ctx.canonical_dir
        output_dir: Path = ctx.fact_dir

        meta_dir: Path = ctx.meta_dir
        stage = "enriched"
        meta = BaseMeta(meta_dir, stage=stage)

        for input_file in list(input_dir.glob(f"*trade.parquet")):
            name = input_file.stem  # sh_trade / sz_trade

            if not meta.upstream_changed(input_file):
                logs.warning(f"[TradeEnrichStep] {name} unchanged -> skip")
                continue
            manifest_path = meta.manifest_path(input_file, 'normalize')

            accessor = SymbolAccessor.from_manifest(manifest_path)

            tables = []

            with self.inst.timer(f"TradeEnrich_{name}"):
                for symbol in accessor.symbols():
                    table = accessor.get(symbol)
                    if table.num_rows == 0:
                        continue
            enriched = self.engine.execute(table)
            tables.append(enriched)

            if not tables:
                logs.warning(f"[TradeEnrich] {name} no data")
                continue
            # --------------------------------------------------
            # 3. 合并 & 写出 fact
            # --------------------------------------------------
            result_table = pa.concat_tables(tables)

            output_file = output_dir / f"{name}_{stage}.parquet"
            pq.write_table(result_table, output_file)

            # --------------------------------------------------
            # 4. 提交 Meta（证明这个结果成立）
            # --------------------------------------------------
            result = MetaResult(
                input_file=input_file,
                output_file=output_file,
                rows=result_table.num_rows,
            )

            meta.commit(result)

            logs.info(
                f"[TradeEnrich] written {output_file.name} "
                f"(rows={result_table.num_rows})"
            )

        return ctx
