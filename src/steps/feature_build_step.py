# src/steps/feature_build_step.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Union

import pyarrow as pa
import pyarrow.parquet as pq

from src.pipeline.step import PipelineStep
from src.pipeline.context import PipelineContext
from src.meta.meta import BaseMeta, MetaResult
from src.utils.logger import logs
from src.meta.symbol_accessor import SymbolAccessor


# -----------------------------------------------------------------------------
# Utility: merge append / replace columns
# -----------------------------------------------------------------------------
def merge_append_replace(
    base: pa.Table,
    delta: pa.Table,
    *,
    only_feature_columns: bool = False,
) -> pa.Table:
    """
    Merge delta columns into base table (append / replace).

    Contract (frozen):
      - base.num_rows == delta.num_rows
      - row order aligned (same minute sequence)
      - delta contains columns to append / replace

    Parameters
    ----------
    only_feature_columns:
      - If True: only merge columns whose name starts with "l0_" / "l1_" / "l2_"
      - This prevents accidental overwrite of fact columns (open/high/low/close/etc.)
    """
    if base.num_rows != delta.num_rows:
        raise ValueError(
            f"[FeatureBuild] row mismatch: base={base.num_rows}, delta={delta.num_rows}"
        )

    out = base
    for name in delta.column_names:
        if only_feature_columns and not (
            name.startswith("l0_") or name.startswith("l1_") or name.startswith("l2_")
        ):
            continue

        col = delta[name]
        if name in out.column_names:
            idx = out.column_names.index(name)
            out = out.set_column(idx, name, col)
        else:
            out = out.append_column(name, col)

    return out


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _exchange_from_min_filename(input_file: Path) -> str:
    """
    Convert fact min parquet file name to exchange key.

    Examples:
      sh_trade.min.parquet      -> sh_trade
      sz_trade.min.parquet      -> sz_trade
      sh_trade.min             -> sh_trade
      sh_trade                 -> sh_trade

    Adjust this function if your naming differs.
    """
    name = input_file.name
    # remove suffixes in order
    if name.endswith(".parquet"):
        name = name[: -len(".parquet")]
    if name.endswith(".min"):
        name = name[: -len(".min")]
    if name.endswith(".trade_min"):
        name = name[: -len(".trade_min")]
    return name


def _stem_for_manifest(input_file: Path) -> str:
    """
    Stem used for looking up upstream manifest.

    Frozen rule:
      upstream manifest stem == exchange key (same as _exchange_from_min_filename)

    Adjust if your upstream min stage commits with a different stem.
    """
    return _exchange_from_min_filename(input_file)


def _normalize_engines(
    engine_or_list: Optional[Union[object, Sequence[object]]]
) -> List[object]:
    if engine_or_list is None:
        return []
    if isinstance(engine_or_list, (list, tuple)):
        return list(engine_or_list)
    return [engine_or_list]


# -----------------------------------------------------------------------------
# FeatureBuildStep (Frozen v2)
# -----------------------------------------------------------------------------
class FeatureBuildStep(PipelineStep):
    """
    FeatureBuildStep (Frozen v2)

    Semantics:
      fact/<exchange>.min.parquet
        -> feature/<exchange>.feature.parquet

    Principles (frozen):
      - Feature table is the final data product
      - L0 / L1 / L2 are column-level evolution stages
      - Per-symbol execution is the only allowed loop
      - Symbol discovery is manifest-driven (Normalize/Min stage produces symbol index)
      - Engines are deterministic (no side effects)
      - Output write is atomic (single parquet write)
      - Meta commit is single transaction per exchange

    Notes:
      - L1 is a list (L1Stat + L1Norm, possibly multiple windows)
      - L2 reserved for future extension
    """

    def __init__(
        self,
        *,
        l0_engine: Optional[object] = None,
        l1_engines: Optional[Sequence[object]] = None,
        l2_engine: Optional[object] = None,
        only_feature_columns: bool = True,
        inst=None,
    ) -> None:
        super().__init__(inst)
        self.l0 = l0_engine
        self.l1s = list(l1_engines) if l1_engines is not None else []
        self.l2 = l2_engine
        self.only_feature_columns = only_feature_columns

    # ------------------------------------------------------------------
    def run(self, ctx: PipelineContext) -> PipelineContext:
        fact_dir: Path = ctx.fact_dir
        feature_dir: Path = ctx.feature_dir
        meta_dir: Path = ctx.meta_dir

        _ensure_dir(feature_dir)

        # stage for this step
        stage = "feature"
        meta_feature = BaseMeta(meta_dir, stage=stage)

        # upstream stage name (where symbol index manifest lives)
        # In your pipeline: MinuteTradeAgg writes *.min.parquet and commits stage="min"
        upstream_stage = "min"
        meta_up = BaseMeta(meta_dir, stage=upstream_stage)

        # ------------------------------------------------------------------
        # Iterate fact inputs (single upstream)
        # ------------------------------------------------------------------
        for input_file in sorted(fact_dir.glob("*.min.parquet")):
            exchange = _exchange_from_min_filename(input_file)

            # --------------------------------------------------------------
            # Upstream check (Meta v1) - sufficient for now
            # --------------------------------------------------------------
            if not meta_feature.upstream_changed(input_file):
                logs.warning(f"[Feature] {exchange} unchanged -> skip")
                continue

            # --------------------------------------------------------------
            # Load fact table (single file)
            # --------------------------------------------------------------
            table = pq.read_table(input_file)
            if table.num_rows == 0:
                logs.warning(f"[Feature] {exchange} empty input -> skip")
                continue

            # --------------------------------------------------------------
            # Resolve manifest -> SymbolAccessor -> bind view
            # --------------------------------------------------------------
            stem = _stem_for_manifest(input_file)
            manifest_path = meta_up.manifest_path(stem)
            if not manifest_path.exists():
                raise FileNotFoundError(
                    f"[FeatureBuild] upstream manifest missing: stage={upstream_stage}, "
                    f"stem={stem}, path={manifest_path}"
                )

            accessor = SymbolAccessor.from_manifest(manifest_path)
            view = accessor.bind(table)  # expected: provides symbols(), get(symbol)

            # --------------------------------------------------------------
            # Build per-symbol features
            # --------------------------------------------------------------
            feature_tables: List[pa.Table] = []

            symbols = list(view.symbols())
            with self.inst.timer(f"FeatureBuild_{exchange}"):
                for symbol in symbols:
                    sub = view.get(symbol)  # O(1), 0-copy slice
                    if sub.num_rows == 0:
                        continue

                    out = sub

                    # L0
                    if self.l0 is not None:
                        delta0 = self.l0.execute(out)
                        out = merge_append_replace(
                            out, delta0, only_feature_columns=self.only_feature_columns
                        )

                    # L1 (multi-engine chain)
                    for eng in self.l1s:
                        delta1 = eng.execute(out)
                        out = merge_append_replace(
                            out, delta1, only_feature_columns=self.only_feature_columns
                        )

                    # L2 (future)
                    if self.l2 is not None:
                        delta2 = self.l2.execute(out)
                        out = merge_append_replace(
                            out, delta2, only_feature_columns=self.only_feature_columns
                        )

                    feature_tables.append(out)

                if not feature_tables:
                    logs.warning(f"[Feature] {exchange} no symbols produced")
                    continue

                # --------------------------------------------------------------
                # Concatenate feature table (symbol blocks already aligned)
                # --------------------------------------------------------------
                result = pa.concat_tables(feature_tables, promote_options="default")

                output_file = feature_dir / f"{exchange}.{stage}.parquet"
                pq.write_table(result, output_file)

                # --------------------------------------------------------------
                # Commit Meta (correct rows & stats)
                # --------------------------------------------------------------
                meta_feature.commit(
                    MetaResult(
                        input_file=input_file,
                        output_file=output_file,
                        rows=result.num_rows,
                        # index optional; feature stage usually doesn't need index
                    )
                )

                logs.info(
                    f"[Feature] written {output_file.name} "
                    f"symbols={len(symbols)} "
                    f"(rows={result.num_rows}, cols={len(result.column_names)})"
                )

        return ctx
