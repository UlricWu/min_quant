# src/steps/minute_trade_agg_step.py
from __future__ import annotations

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.engines.minute_trade_agg_engine import MinuteTradeAggEngine
from src.meta.meta import BaseMeta, MetaResult
from src.meta.symbol_accessor import SymbolAccessor
from src.utils.logger import logs
import pyarrow.compute as pc


class MinuteTradeAggStep(PipelineStep):
    """
    MinuteTradeAggStep (Frozen)

    Semantics:
      fact/{date}.enriched.parquet   (multi-symbol, event-level)
        -> fact/{date}.min.parquet   (multi-symbol, minute-level)
    """

    def __init__(self, engine: MinuteTradeAggEngine, inst=None) -> None:
        super().__init__(inst)
        self.engine = engine

    def run(self, ctx: PipelineContext) -> PipelineContext:
        input_dir: Path = ctx.fact_dir
        stage = 'min'

        meta = BaseMeta(ctx.meta_dir, stage=stage)

        for input_file in input_dir.glob("*.enriched.parquet"):
            name = input_file.stem.split(".")[0]

            # --------------------------------------------------
            # 1. upstream check
            # --------------------------------------------------
            if not meta.upstream_changed(input_file):
                logs.info(f"[MinuteTradeAgg] {name} unchanged -> skip")
                continue

            # --------------------------------------------------
            # 2. 从 normalize manifest 构造 symbol accessor
            # --------------------------------------------------
            # manifest_path = meta.manifest_path(input_file, stage="normalize")
            # accessor = SymbolAccessor.from_manifest(manifest_path)
            #
            minute_tables = []

            # 1. 读取 enriched 表（一次）
            enriched = pq.read_table(input_file)

            # 2. 从 normalize manifest 构造 accessor
            manifest_path = meta.manifest_path(input_file, stage="normalize")
            accessor = SymbolAccessor.from_manifest(manifest_path)
            # 3. 绑定 enriched 表
            view = accessor.bind(enriched)

            # --------------------------------------------------
            # 3. per-symbol minute aggregation
            # --------------------------------------------------
            with self.inst.timer(f"MinuteTradeAgg_{name}"):

                for symbol in view.symbols():
                    trade = view.get(symbol)
                    if trade.num_rows == 0:
                        continue

                    minute = self.engine.execute(trade)
                    if minute.num_rows == 0:
                        continue

                    minute = minute.append_column(
                        "symbol",
                        pa.array([symbol] * minute.num_rows)
                    )

                    minute_tables.append(minute)

                if not minute_tables:
                    logs.warning(f"[MinuteTradeAgg] {name} no minute data")
                    continue

                # --------------------------------------------------
                # 4. 合并为全市场 minute 表
                # --------------------------------------------------
                result = pa.concat_tables(minute_tables)

                index = self.build_symbol_slice_index(result)

                output_file = input_dir / f"{name}.{stage}.parquet"
                pq.write_table(result, output_file)

                # --------------------------------------------------
                # 5. commit meta
                # --------------------------------------------------
                meta.commit(
                    MetaResult(
                        input_file=input_file,
                        output_file=output_file,
                        rows=result.num_rows,
                        index=index
                    )
                )

                logs.info(
                    f"[MinuteTradeAgg] written {output_file.name} "
                    f"symbols={len(view.symbols())} "
                    f"(rows={result.num_rows})"
                )

        return ctx

    @staticmethod
    def build_symbol_slice_index(sorted_table: pa.Table) -> dict[str, tuple[int, int]]:
        if sorted_table.num_rows == 0:
            return {}

        sym = sorted_table["symbol"]

        if not pa.types.is_string(sym.type):
            sym = pa.compute.cast(sym, pa.string())

        ree = pa.compute.run_end_encode(sym).combine_chunks()

        values = ree.values.to_pylist()
        run_ends = ree.run_ends.to_pylist()

        index: dict[str, tuple[int, int]] = {}
        start = 0

        for symbol, end in zip(values, run_ends):
            end = int(end)
            index[symbol] = (start, end - start)
            start = end

        return index
